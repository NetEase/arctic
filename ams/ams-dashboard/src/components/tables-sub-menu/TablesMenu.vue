<template>
  <div class="tables-menu g-flex">
    <div class="database-list">
      <div class="select-catalog g-flex-jsb">
        <span class="label">{{$t('cluster')}}</span>
        <a-select
          v-model:value="curCatalog"
          :options="catalogOptions"
          @change="catalogChange"
          :loading="catalogLoading"
          :getPopupContainer="getPopupContainer"
          />
      </div>
      <div class="list-wrap">
        <div class="add g-flex-jsb">
          <span class="label">{{$t('database', 2)}}</span>
          <!-- <plus-outlined @click="addDatabase" class="icon" /> -->
        </div>
        <a-list
          :data-source="databaseList"
          :loading="loading"
        >
          <template #renderItem="{ item }">
            <a-list-item  @mouseenter="mouseEnter(item)" :class="{ active: database === item }">
              <svg-icon icon-class="database" class="g-mr-8" />
              <p :title="item" class="name g-text-nowrap">
                {{ item }}
              </p>
            </a-list-item>
          </template>
        </a-list>
      </div>
    </div>
    <div class="table-list">
      <div class="select-catalog"></div>
      <div class="list-wrap">
        <div class="add g-flex-jsb">
          <span class="label">{{$t('table', 2)}}</span>
          <!-- <plus-outlined @click="createTable" class="icon" /> -->
        </div>
        <a-list
          :data-source="tableList"
          :loading="tableLoading"
        >
          <template #renderItem="{ item }">
            <a-list-item @click="handleClickTable(item)" :class="{ active: tableName === item }">
              <table-outlined class="g-mr-8" />
              <p :title="item" class="name g-text-nowrap">
                {{ item }}
              </p>
            </a-list-item>
          </template>
        </a-list>
      </div>
    </div>
  </div>
  <createDB-modal :visible="showCreateDBModal" :catalogOptions="catalogOptions" @cancel="cancel"></createDB-modal>
</template>

<script lang="ts">
import { defineComponent, onBeforeMount, reactive, toRefs } from 'vue'
import {
  // PlusOutlined,
  TableOutlined
} from '@ant-design/icons-vue'
import CreateDBModal from './CreateDB.vue'
import { useRoute, useRouter } from 'vue-router'
import useStore from '@/store/index'
import { getCatalogList, getDatabaseList, getTableList } from '@/services/table.service'
import { ICatalogItem, ILableAndValue } from '@/types/common.type'
import { debounce } from '@/utils/index'

export default defineComponent({
  name: 'TablesMenu',
  components: {
    // PlusOutlined,
    TableOutlined,
    CreateDBModal
  },
  emits: ['goCreatePage'],
  setup(props, { emit }) {
    const router = useRouter()
    const route = useRoute()
    const store = useStore()
    const state = reactive({
      catalogLoading: false as boolean,
      curCatalog: '',
      database: '',
      tableName: '',
      catalogOptions: [] as ILableAndValue[],
      showCreateDBModal: false,
      loading: false,
      tableLoading: false,
      databaseList: [] as string[],
      tableList: [] as string[]
    })

    const mouseEnter = debounce((item: string) => {
      if (state.database === item) {
        return
      }
      state.database = item
      state.tableName = ''
      getAllTableList()
    })

    const toggleTablesMenu = (flag = false) => {
      store.updateTablesMenu(flag)
    }

    const getPopupContainer = (triggerNode: Element) => {
      return triggerNode.parentNode
    }

    const clickDatabase = () => {

    }
    const catalogChange = (value: string) => {
      state.curCatalog = value
      state.databaseList.length = 0
      state.tableList.length = 0
      getAllDatabaseList()
    }
    const addDatabase = () => {
      state.showCreateDBModal = true
    }
    const cancel = () => {
      state.showCreateDBModal = false
    }
    const createTable = () => {
      emit('goCreatePage')
    }
    const handleClickTable = (item: string) => {
      state.tableName = item
      store.updateTablesMenu(false)
      const pathQuery = {
        path: '/tables',
        query: {
          catalog: state.curCatalog,
          db: state.database,
          table: state.tableName
        }
      }
      if (route.path.indexOf('tables') > -1) {
        router.replace(pathQuery)
        return
      }
      router.push(pathQuery)
    }

    const getCatalogOps = () => {
      state.catalogLoading = true
      getCatalogList().then((res: ICatalogItem[]) => {
        if (!res) {
          return
        }
        state.catalogOptions = (res || []).map((ele: ICatalogItem) => ({
          value: ele.catalogName,
          label: ele.catalogName
        }))
        if (state.catalogOptions.length) {
          const query = route.query
          state.curCatalog = (query?.catalog)?.toString() || state.catalogOptions[0].value
        }
        getAllDatabaseList()
      }).finally(() => {
        state.catalogLoading = false
      })
    }

    const getAllDatabaseList = () => {
      state.loading = true
      getDatabaseList(state.curCatalog).then((res: string[]) => {
        state.databaseList = res || []
        if (state.databaseList.length) {
          state.database = state.databaseList[0]
          getAllTableList()
        }
      }).finally(() => {
        state.loading = false
      })
    }

    const getAllTableList = () => {
      state.tableLoading = true
      getTableList({
        catalog: state.curCatalog,
        db: state.database
      }).then((res: string[]) => {
        state.tableList = res || []
      }).finally(() => {
        state.tableLoading = false
      })
    }
    onBeforeMount(() => {
      getCatalogOps()
    })

    return {
      ...toRefs(state),
      mouseEnter,
      getPopupContainer,
      toggleTablesMenu,
      clickDatabase,
      catalogChange,
      addDatabase,
      cancel,
      createTable,
      handleClickTable
    }
  }
})
</script>

<style lang="less" scoped>
  .tables-menu {
    box-sizing: border-box;
    height: 100%;
    width: 512px;
    box-shadow: rgb(0 21 41 / 8%) 2px 0 6px;
    .database-list,
    .table-list {
      flex: 1;
      background-color: #fff;
      padding-top: 4px;
    }
    .table-list,
    .database-list .list-wrap {
      border-right: 1px solid #e8e8f0;
    }
    .list-wrap {
      height: calc(100% - 48px);
    }
    :deep(.ant-list) {
      padding: 0 4px;
      height: calc(100% - 48px);
      margin-top: 4px;
      overflow-y: auto;
    }
    .select-catalog,
    .add {
      align-items: center;
      height: 40px;
      padding: 0 12px;
    }
    .database-list .select-catalog {
      padding-right: 4px;
    }
    .add {
      margin: 4px 4px 0;
      background-color: #f6f7fa;
    }
    :deep(.select-catalog .ant-select) {
      flex: 1;
      margin-left: 12px;
    }
    .icon {
      cursor: pointer;
    }
    .label {
      font-size: 16px;
      font-weight: 600;
    }
    :deep(.ant-list-items) {
      padding: 4px 0;
    }
    :deep(.ant-list-item) {
      justify-content: flex-start;
      padding: 12px;
      color: #102048;
      height: 40px;
      cursor: pointer;
      &.active,
      &:hover {
        background-color: #f6f7fa;
        color: @primary-color;
      }
    }
    .ant-list-split .ant-list-item {
      border-bottom: 0;
    }
    .name {
      max-width: 200px;
    }
  }

</style>
