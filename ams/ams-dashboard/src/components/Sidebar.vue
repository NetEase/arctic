<template>
  <div class="side-bar">
    <div :class="{'logo-collapsed': collapsed}" @mouseenter="toggleTablesMenu(false)" class="logo g-flex-ac">
      <img src="../assets/images/logo.svg" class="logo-img" alt="">
      <img v-show="!collapsed" src="../assets/images/arctic-dashboard.svg" class="arctic-name" alt="">
    </div>
    <a-menu
      v-model:selectedKeys="selectedKeys"
      mode="inline"
      theme="dark"
      :inline-collapsed="collapsed"
    >
      <a-menu-item v-for="item in menuList" :key="item.key" @click="navClick(item)" @mouseenter="mouseenter(item)" :class="{'active-color': (store.isShowTablesMenu && item.key === 'tables')}">
        <template #icon>
          <DashboardOutlined v-if="item.icon === 'DashboardOutlined'" />
          <TableOutlined v-if="item.icon === 'TableOutlined'" />
          <svg-icon v-if="item.icon === 'ScheduleOutlined'" icon-class="optimizing" class="svg-icon" />
          <SettingOutlined v-if="item.icon === 'SettingOutlined'" />
          <SettingOutlined v-if="item.icon === 'SettingOutlined'" />
          <svg-icon v-if="item.icon === 'ConsoleSqlOutlined'" icon-class="terminal" class="svg-icon" />
        </template>
        <span>{{ $t(item.title) }}</span>
      </a-menu-item>
    </a-menu>
    <a-button type="link" @click="toggleCollapsed" class="toggle-btn">
      <MenuUnfoldOutlined v-if="collapsed" />
      <MenuFoldOutlined v-else />
    </a-button>
    <div @click.self="toggleTablesMenu(false)" v-if="store.isShowTablesMenu" @mouseleave="toggleTablesMenu(false)" :class="{'collapsed-sub-menu': collapsed}" class="tables-menu-wrap">
      <TableMenu @goCreatePage="goCreatePage" />
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent, nextTick, reactive, toRefs, watchEffect } from 'vue'
import {
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  DashboardOutlined,
  TableOutlined,
  SettingOutlined
} from '@ant-design/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import useStore from '@/store/index'
import TableMenu from '@/components/tables-sub-menu/TablesMenu.vue'
import { useI18n } from 'vue-i18n'

interface MenuItem {
  key: string
  title: string
  icon: string
}

export default defineComponent({
  name: 'Sidebar',
  components: {
    MenuFoldOutlined,
    MenuUnfoldOutlined,
    DashboardOutlined,
    TableOutlined,
    SettingOutlined,
    TableMenu
  },
  setup () {
    const { t } = useI18n()
    const router = useRouter()
    const route = useRoute()
    const store = useStore()

    const state = reactive({
      collapsed: false,
      selectedKeys: [] as string[],
      menuList: [
        // {
        //   key: 'overview',
        //   title: t('overview'),
        //   icon: 'DashboardOutlined'
        // },
        {
          key: 'tables',
          title: t('tables'),
          icon: 'TableOutlined'
        },
        {
          key: 'optimizers',
          title: t('optimizers'),
          icon: 'ScheduleOutlined'
        },
        {
          key: 'terminal',
          title: t('terminal'),
          icon: 'ConsoleSqlOutlined'
        }
        // {
        //   key: 'settings',
        //   title: t('settings'),
        //   icon: 'SettingOutlined'
        // }
      ] as MenuItem[]
    })
    const setCurMenu = () => {
      const pathArr = route.path.split('/')
      if (route.path) {
        state.selectedKeys = [pathArr[1]]
      }
    }
    watchEffect(() => {
      setCurMenu()
    })
    const toggleCollapsed = () => {
      state.collapsed = !state.collapsed
      // The stretch animation is 300 ms and the trigger method needs to be delayed here
      setTimeout(() => {
        window.dispatchEvent(new Event('resize'))
      }, 300)
    }

    const navClick = (item:MenuItem) => {
      if (item.key === 'tables') {
        nextTick(() => {
          setCurMenu()
        })
        return
      }
      router.replace({
        path: `/${item.key}`
      })
    }

    const mouseenter = (item:MenuItem) => {
      toggleTablesMenu(item.key === 'tables')
    }

    const goCreatePage = () => {
      toggleTablesMenu(false)
      router.push({
        path: '/tables/create'
      })
    }

    const toggleTablesMenu = (flag = false) => {
      store.updateTablesMenu(flag)
    }

    return {
      ...toRefs(state),
      toggleCollapsed,
      navClick,
      mouseenter,
      store,
      toggleTablesMenu,
      goCreatePage
    }
  }
})
</script>

<style lang="less" scoped>
  .side-bar {
    position: relative;
    height: 100%;
    transition: width 0.3s;
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
    :deep(.ant-menu) {
      height: 100%;
      width: 200px;
      &.ant-menu-inline-collapsed {
        width: 80px;
      }
    }
    :deep(.ant-menu-item) {
      margin: 0;
      padding-left: 30px !important;
      .ant-menu-title-content {
        width: 100%;
        margin-left: 12px;
      }
      &.active {
        background-color: @primary-color;
        color: #fff;
      }
      &.active-color {
        color: #fff;
      }
      &:hover {
        color: #fff;
      }
    }
    .logo {
      padding: 12px 0 18px 24px;
      overflow: hidden;
      background-color: #001529;
    }
    .logo-img {
      width: 32px;
      height: 32px;
    }
    .arctic-name {
      width: 112px;
      margin: 4px 0 0 8px;
    }
    .toggle-btn {
      position: absolute;
      right: -68px;
      top: 8px;
      font-size: 18px;
      padding: 0 24px;
    }
  }
  .tables-menu-wrap {
    position: absolute;
    top: 0;
    left: 200px;
    right: 0;
    bottom: 0;
    z-index: 100;
    &.collapsed-sub-menu {
      left: 80px;
    }
  }
</style>
