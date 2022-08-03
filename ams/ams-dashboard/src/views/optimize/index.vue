<template>
  <div class="optimize-wrap">
    <div class="optimize-group g-flex-ac">
      <div class="left-group">
        <span class="g-mr-16">{{$t('optimzerGroup')}}</span>
        <a-select
          v-model:value="curGroupName"
          :showSearch="true"
          :options="groupList"
          :placeholder="placeholder.selectOptGroupPh"
          @change="onChangeGroup"
          style="width: 240px"
        />
      </div>
      <div class="btn-wrap">
        <span class="g-ml-16 f-shink-0">{{$t('resourceOccupation')}}  <span class="text-color">{{groupInfo.occupationCore}}</span> {{$t('core')}} <span class="text-color">{{groupInfo.occupationMemory}}</span> G </span>
        <a-button type="primary" @click="expansionJob" class="g-ml-8">{{$t('scaleOut')}}</a-button>
      </div>
    </div>
    <div class="content">
      <a-tabs v-model:activeKey="activeTab" destroyInactiveTabPane>
        <a-tab-pane
          v-for="tab in tabConfig"
          :key="tab.value"
          :tab="tab.label"
          :class="[activeTab === tab.value ? 'active' : '']"
          >
          <List :curGroupName="curGroupName" :type="tab.value" :needFresh="needFresh" />
        </a-tab-pane>
      </a-tabs>
    </div>
  </div>
  <scale-out-modal
    v-if="showScaleOutModal"
    :visible="showScaleOutModal"
    :resourceGroup="curGroupName === 'all' ? '' : curGroupName"
    @cancel="showScaleOutModal = false"
    @refreshOptimizersTab="refreshOptimizersTab"
   />
</template>

<script lang="ts">
import { IGroupItem, IGroupItemInfo, ILableAndValue, IMap } from '@/types/common.type'
import { computed, defineComponent, onMounted, reactive, shallowReactive, toRefs } from 'vue'
import { useI18n } from 'vue-i18n'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import { usePagination } from '@/hooks/usePagination'
import { getOptimizerGroups, getQueueResourceInfo } from '@/services/optimize.service'
import ScaleOutModal from './components/ScaleOut.vue'
import List from './components/List.vue'

export default defineComponent({
  name: 'Optimize',
  components: {
    List,
    ScaleOutModal
  },
  setup() {
    const { t } = useI18n()

    const tabConfig: ILableAndValue[] = shallowReactive([
      { label: t('optimizers'), value: 'optimizers' },
      { label: t('tables'), value: 'tables' }
    ])
    const placeholder = reactive(usePlaceholder())
    const pagination = reactive(usePagination())
    const state = reactive({
      curGroupName: 'all' as string,
      groupList: [
        {
          label: t('allGroups'),
          value: 'all'
        }
      ] as IMap<string | number>[],
      groupInfo: {
        occupationCore: 0,
        occupationMemory: 0
      } as IGroupItemInfo,
      activeTab: 'tables' as string,
      showScaleOutModal: false as boolean,
      needFresh: false as boolean
    })

    const isTableTab = computed(() => {
      return (state.activeTab === 'tables')
    })

    const onChangeGroup = () => {
      getCurGroupInfo()
    }

    const getCompactQueues = async() => {
      const result = await getOptimizerGroups();
      (result || []).forEach((item: IGroupItem) => {
        state.groupList.push({
          label: item.optimizerGroupName,
          value: item.optimizerGroupName
        })
      })
    }

    const getCurGroupInfo = async() => {
      const result = await getQueueResourceInfo(state.curGroupName || '')
      state.groupInfo = { ...result }
    }

    const expansionJob = () => {
      state.showScaleOutModal = true
    }

    const refreshOptimizersTab = () => {
      state.activeTab = 'optimizers'
      state.needFresh = true
    }

    onMounted(() => {
      getCompactQueues()
      getCurGroupInfo()
    })

    return {
      isTableTab,
      placeholder,
      pagination,
      ...toRefs(state),
      tabConfig,
      onChangeGroup,
      expansionJob,
      refreshOptimizersTab
    }
  }
})

</script>

<style lang="less" scoped>
.optimize-wrap {
  border: 1px solid #e5e5e5;
  padding: 12px 0;
  height: 100%;
  overflow-y: auto;
  .optimize-group {
    justify-content: space-between;
    padding: 0 24px;
    .f-shink-0 {
      flex-shrink: 0;
    }
    .text-color {
      color: #0ad787;
    }
  }
  .status-icon {
    width: 8px;
    height: 8px;
    border-radius: 8px;
  }
  .tabs {
    height: 32px;
    display: flex;
    align-items: center;
    margin-bottom: 12px;
    padding: 0 12px;
    border: 1px solid #e5e5e5;
  }
  :deep(.ant-tabs-content-holder) {
    padding: 0 24px;
  }
  :deep(.ant-tabs-nav) {
    padding: 0 12px;
  }
  .table-name {
    color: @primary-color;
    &:hover {
      cursor: pointer;
    }
  }
}
</style>
