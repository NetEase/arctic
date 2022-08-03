<template>
  <div class="introduce-wrap">
    <p class="desc">
      Welcome to arctic, arctic is a streaming lake warehouse system open
      sourced by NetEase. Arctic adds more real-time capabilities on top of
      iceberg and hive, and provides stream-batch unified, out-of-the-box
      metadata services for dataops, allowing Data lakes much more usable and
      practical.
    </p>
    <h2 class="sub-title">What is arctic</h2>
    <p class="desc">
      Arctic is a streaming lakehouse service built on top of apache iceberg
      table format. Through arctic, users could benefit optimized CDC、streaming
      update、fresh olap etc. on engines like flink, spark, and trino. Combined
      with efficient offline processing capabilities of data lakes, arctic can
      serve more scenarios where streaming and batch are fused. At the same
      time, the function of self-optimization、concurrent conflict resolution
      and standard management tools could effectively reduce the burden on users
      in data lake management and optimization.
    </p>
    <img
      src="@/assets/images/arctic-introduce.png"
      alt=""
      class="arctic-introduce"
    />
    <p class="desc">
      Arctic services are presented by deploying AMS, which can be considered as
      a replacement for HMS (Hive Metastore), or HMS for iceberg. Arctic uses
      iceberg as the base table format, but instead of hacking the iceberg
      implementation, it uses iceberg as a lib. Arctic's open overlay
      architecture can help large-scale offline data lakes quickly upgraded to
      real-time data lakes, without worrying about compatibility issues with the
      original data lakes, allowing data lakes to meet more real-time analysis,
      real-time risk control, Real-time training, feature engineering and other
      scenarios.
    </p>
    <h2 class="sub-title">Arctic features</h2>
    <ol>
      <li>Efficient streaming updates based on primary keys</li>
      <li>Data auto bucketing and self-optimized for performance and efficiency</li>
      <li>Encapsulating data lake and message queue into a unified table to achieve lower-latency computing
      </li>
      <li>Provide standardized metrics, dashboard and related management tools for streaming lakehouse</li>
      <li>Support spark and flink to read and write data, support trino to query data</li>
      <li>100% compatible with iceberg / hive table format and syntax</li>
      <li>Provide transactional guarantee for streaming and batch concurrent writing</li>
    </ol>
    <h2 class="sub-title">Engines supported</h2>
    <p class="desc">Arctic support multiple processing engines as below:</p>
    <table class="table">
      <thead class="ant-table-thead">
        <tr>
          <th>Processing Engine</th>
          <th>Version</th>
        </tr>
      </thead>
      <tbody class="ant-table-tbody">
        <tr>
          <td>
            <span class="td-val">Flink</span>
          </td>
          <td>
            <span class="td-val">1.12.x and 1.14.x</span>
          </td>
        </tr>
        <tr>
          <td>
            <span class="td-val">Spark</span>
          </td>
          <td>
            <span class="td-val">3.1+</span>
          </td>
        </tr>
        <tr>
          <td>
            <span class="td-val">Trino</span>
          </td>
          <td>
            <span class="td-val">380</span>
          </td>
        </tr>
      </tbody>
    </table>
    <h2 class="sub-title">Quickstart</h2>
    <p class="desc">
      Visit
      <a
        href="https://arctic.netease.com/ch/getting-started/"
        target="_blank"
        rel="noopener noreferrer">
      https://arctic.netease.com/ch/getting-started/
      </a>
      to quickly explore what arctic can do.
    </p>
  </div>
</template>

<script lang="ts">
import { defineComponent, onMounted } from 'vue'

export default defineComponent({
  name: 'Introduce',
  components: {},
  setup() {
    onMounted(() => {})
    return {}
  }
})
</script>

<style lang="less" scoped>
.introduce-wrap {
  font-size: 16px;
  box-sizing: border-box;
  padding: 0 12px;
  .title {
    font-size: 30px;
    font-weight: bold;
  }
  .arctic-introduce {
    width: 80%;
    margin: 0 auto 16px;
    display: block;
  }
  .desc {
    margin-bottom: 16px;
  }
  .sub-title {
    margin: 24px 0 16px;
    font-size: 24px;
    font-weight: bold;
    border-bottom: 1px solid #e5e5e5;
    padding-bottom: 7px;
  }
  code {
    padding: 2px 4px;
    background-color: rgba(175, 184, 193, 0.2);
    border-radius: 4px;
    display: inline-block;
    line-height: 14px;
    font-size: 14px;
  }
  ol li {
    line-height: 24px;
    margin-top: 4px;
    padding-left: 32px;
    position: relative;
    &::after {
      content: "";
      position: absolute;
      width: 6px;
      height: 6px;
      border-radius: 100%;
      background-color: #000;
      left: 14px;
      top: 10px;
    }
  }
}
</style>
