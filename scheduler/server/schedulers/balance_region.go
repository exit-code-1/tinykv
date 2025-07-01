// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// 1. 获取所有可用的 store，并按 region size 降序排列
	stores := cluster.GetStores()
	var candidates []*core.StoreInfo
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			candidates = append(candidates, store)
		}
	}
	if len(candidates) < 2 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].GetRegionSize() > candidates[j].GetRegionSize()
	})

	// 2. 尝试从 region size 最大的 store 迁移 region
	for i := len(candidates) - 1; i >= 0; i-- { // 从大到小遍历
		srcStore := candidates[i]

		var region *core.RegionInfo
		// 1. 优先选择 pending region
		cluster.GetPendingRegionsWithLock(srcStore.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})

		// 2. 其次选择 follower region（如果没找到）
		if region == nil {
			cluster.GetFollowersWithLock(srcStore.GetID(), func(container core.RegionsContainer) {
				region = container.RandomRegion(nil, nil)
			})
		}

		// 3. 最后选择 leader region（如果没找到）
		if region == nil {
			cluster.GetLeadersWithLock(srcStore.GetID(), func(container core.RegionsContainer) {
				region = container.RandomRegion(nil, nil)
			})
		}

		// 3. 选择目标 store（region size 最小的 store，且不能是自己，且不能已存在 peer）
		var dstStore *core.StoreInfo
		for _, store := range candidates {
			if store.GetID() == srcStore.GetID() {
				continue
			}
			if region.GetStorePeer(store.GetID()) != nil {
				continue
			}
			dstStore = store
			break
		}
		if dstStore == nil {
			continue
		}

		// 4. 判断是否值得迁移
		srcSize := srcStore.GetRegionSize()
		dstSize := dstStore.GetRegionSize()
		regionSize := region.GetApproximateSize()
		if regionSize == 0 {
			regionSize = 1 // 防止除零
		}
		if srcSize-dstSize < 2*regionSize {
			continue
		}

		// 5. 创建迁移操作
		newPeer, err := cluster.AllocPeer(dstStore.GetID())
		if err != nil {
			continue
		}
		op, err := operator.CreateMovePeerOperator(
			"balance-region",
			cluster,
			region,
			operator.OpBalance,
			srcStore.GetID(),
			newPeer.GetStoreId(),
			newPeer.GetId(),
		)
		if err != nil {
			continue
		}
		return op
	}
	return nil
}
