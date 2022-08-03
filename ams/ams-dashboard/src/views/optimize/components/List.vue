<template>
  <div class="list-wrap">
    <a-table
      class="ant-table-common"
      :columns="columns"
      :data-source="dataSource"
      :pagination="pagination"
      :loading="loading"
      @change="changeTable"
      >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'tableName'">
          <span class="primary-link" @click="goTableDetail(record)">
            {{ record.tableName }}
          </span>
        </template>
        <template v-if="column.dataIndex === 'durationDisplay'">
          <span :title="record.durationDesc">
            {{ record.durationDisplay }}
          </span>
        </template>
        <template v-if="column.dataIndex === 'operation'">
          <span class="primary-link" @click="releaseModal(record)">
            {{ t('release') }}
          </span>
        </template>
      </template>
    </a-table>
  </div>
</template>
<script lang="ts" setup>
import { computed, onMounted, reactive, ref, shallowReactive, watch } from 'vue'
import { IOptimizeResourceTableItem, IOptimizeTableItem } from '@/types/common.type'
import { getOptimizerResourceList, getOptimizerTableList, releaseResource } from '@/services/optimize.service'
import { useI18n } from 'vue-i18n'
import { usePagination } from '@/hooks/usePagination'
import { bytesToSize, formatMS2Time, mbToSize, formatMS2DisplayTime } from '@/utils'
import { Modal } from 'ant-design-vue'
import { useRouter } from 'vue-router'

const { t } = useI18n()
const router = useRouter()

const props = defineProps<{ curGroupName: string, type: string, needFresh: boolean }>()

const loading = ref<boolean>(false)
const tableColumns = shallowReactive([
  { dataIndex: 'tableName', title: t('table'), ellipsis: true, scopedSlots: { customRender: 'tableName' } },
  { dataIndex: 'optimizeStatus', title: t('status'), width: '10%', ellipsis: true },
  { dataIndex: 'durationDisplay', title: t('duration'), width: '10%', ellipsis: true },
  { dataIndex: 'fileCount', title: t('fileCount'), width: '10%', ellipsis: true },
  { dataIndex: 'fileSizeDesc', title: t('fileSize'), width: '10%', ellipsis: true },
  { dataIndex: 'quota', title: t('quota'), width: '10%', ellipsis: true },
  { dataIndex: 'quotaOccupationDesc', title: t('quotaOccupation'), width: 160, ellipsis: true }
])
const optimizerColumns = shallowReactive([
  { dataIndex: 'index', title: t('order'), width: 80, ellipsis: true },
  { dataIndex: 'groupName', title: t('optimizerGroup'), ellipsis: true },
  { dataIndex: 'container', title: t('container'), ellipsis: true },
  { dataIndex: 'jobStatus', title: t('status'), ellipsis: true },
  { dataIndex: 'resourceAllocation', title: t('resourceAllocation'), width: '20%', ellipsis: true },
  { dataIndex: 'operation', title: t('operation'), key: 'operation', ellipsis: true, width: 160, scopedSlots: { customRender: 'operation' } }
])
const pagination = reactive(usePagination())
const optimizersList = reactive<IOptimizeResourceTableItem[]>([])
const tableList = reactive<IOptimizeTableItem[]>([])

const columns = computed(() => {
  return props.type === 'optimizers' ? optimizerColumns : tableColumns
})

const dataSource = computed(() => {
  return props.type === 'optimizers' ? optimizersList : tableList
})

watch(
  () => props.curGroupName,
  (value) => {
    value && refresh()
  }
)

watch(
  () => props.needFresh,
  (value) => {
    value && refresh(true)
  }
)

function refresh(resetPage?: boolean) {
  if (resetPage) {
    pagination.current = 1
  }
  if (props.type === 'optimizers') {
    getOptimizersList()
  } else {
    getTableList()
  }
}

async function getOptimizersList () {
  try {
    optimizersList.length = 0
    loading.value = true
    const params = {
      optimizerGroup: props.curGroupName,
      page: pagination.current,
      pageSize: pagination.pageSize
    }
    const result = await getOptimizerResourceList(params)
    const { list, total } = result
    pagination.total = total;
    (list || []).forEach((p: IOptimizeResourceTableItem, index: number) => {
      p.resourceAllocation = `${p.coreNumber}${t('core')} ${mbToSize(p.memory)}`
      p.index = (pagination.current - 1) * pagination.pageSize + index + 1
      optimizersList.push(p)
    })
  } catch (error) {
  } finally {
    loading.value = false
  }
}

async function getTableList () {
  try {
    tableList.length = 0
    loading.value = true
    const params = {
      optimizerGroup: props.curGroupName || '',
      page: pagination.current,
      pageSize: pagination.pageSize
    }
    const result = await getOptimizerTableList(params)
    const { list, total } = result
    pagination.total = total;
    (list || []).forEach((p: IOptimizeTableItem) => {
      p.quotaOccupationDesc = p.quotaOccupation ? `${(p.quotaOccupation * 100)}%` : '0'
      p.durationDesc = formatMS2Time(p.duration || 0)
      p.durationDisplay = formatMS2DisplayTime(p.duration || 0)
      p.fileSizeDesc = bytesToSize(p.fileSize)
      tableList.push(p)
    })
  } catch (error) {
  } finally {
    loading.value = false
  }
}

function releaseModal (record: IOptimizeResourceTableItem) {
  Modal.confirm({
    title: t('releaseOptModalTitle'),
    content: '',
    okText: '',
    cancelText: '',
    onOk: () => {
      releaseJob(record)
    }
  })
}
async function releaseJob (record: IOptimizeResourceTableItem) {
  await releaseResource({
    optimizerGroup: record.groupName,
    jobId: record.jobId
  })
  refresh(true)
}
function changeTable ({ current = pagination.current, pageSize = pagination.pageSize }) {
  pagination.current = current
  const resetPage = pageSize !== pagination.pageSize
  pagination.pageSize = pageSize
  refresh(resetPage)
}

function goTableDetail (record: IOptimizeTableItem) {
  const { catalog, database, tableName } = record.tableIdentifier
  router.push({
    path: '/tables',
    query: {
      catalog,
      db: database,
      table: tableName
    }
  })
}

onMounted(() => {
  refresh()
})
</script>
<style lang="less" scoped>
.list-wrap {
  .primary-link {
    color: @primary-color;
    &:hover {
      cursor: pointer;
    }
  }
}
</style>
