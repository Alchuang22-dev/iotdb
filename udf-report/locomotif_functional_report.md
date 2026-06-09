# LoCoMotif UDF 功能实验结果

## 实验准备

- Apache IoTDB: 2.0.7-SNAPSHOT 本地 all-bin/cli-bin
- UDF: `org.apache.iotdb.library.dmatch.UDTFLoCoMotif`，注册名 `locomotif`
- 输出目录: `/Users/alan671/Documents/iotdb-release/iotdb/udf-report/locomotif-functional/20260605152211`
- 数据集:
  - TSMD Benchmark Real Collection: `mitdb1_0.csv`, `ppg_0.csv`, `refit_0.csv` slice
  - PAMAP2: `subject101_activity_sample.csv`，活动 ID 4/5/12/13 的 IMU 加速度子集
  - REFIT: `CLEAN_House1_head_20000.csv` 中前 5000 行

## 实验结果

| 实验 | 状态 | 返回 motif 行数 | Shell 耗时(s) | CLI 统计耗时 | 说明 |
|---|---:|---:|---:|---:|---|
| `synthetic_pair` | PASS | 1 | 0 | 0.067s | 合成数据；4 次重复长度 12 motif；双变量；关闭 warping |
| `tsmd_mitdb1_ecg` | PASS | 2 | 3 | 2.034s | TSMD mitdb1_0；单变量 ECG；窗口 5000 |
| `tsmd_ppg` | PASS | 2 | 3 | 1.902s | TSMD ppg_0；单变量 PPG；窗口 5000 |
| `pamap2_multivariate` | PASS | 2 | 2 | 1.740s | PAMAP2 subject101；9 维 IMU 加速度；窗口 3000 |
| `tsmd_refit_load` | PASS | 3 | 6 | 5.258s | TSMD REFIT refit_0 motif 区间；单变量负载；窗口 7000 |
| `refit_house1_load` | PASS | 2 | 7 | 5.964s | REFIT House1 原始清洗数据前 5000 行；总功率和 3 个电器通道 |

## 结论

- 合成重复片段用于验证基础 UDF 注册、窗口访问和 JSON 输出链路。
- TSMD ECG/PPG 用于验证 LoCoMotif 在真实生理周期信号上发现重复 motif 的能力。
- PAMAP2 使用 9 个 IMU 加速度通道，验证多变量输入路径。
- REFIT 使用电力负载序列，验证长周期/稀疏重复模式场景。

## 结果文件

- Setup SQL: `generated/setup.sql`
- Setup log: `logs/setup.log`
- Summary CSV: `summary.csv`
- Query logs: `results/*.log`

## 实验图示

- 蓝色曲线表示原始序列，不同颜色曲线表示不同 `motifId` 的 member 区间。
- 半透明背景表示对应颜色 motif 的 representative 区间。
- 完整图示索引: `locomotif_functional_figures.md`

![LoCoMotif functional summary](figures/locomotif_functional_summary.png)

### `synthetic_pair`

![synthetic_pair](figures/synthetic_pair.png)

### `tsmd_mitdb1_ecg`

![tsmd_mitdb1_ecg](figures/tsmd_mitdb1_ecg.png)

### `tsmd_ppg`

![tsmd_ppg](figures/tsmd_ppg.png)

### `pamap2_multivariate`

![pamap2_multivariate](figures/pamap2_multivariate.png)

### `tsmd_refit_load`

![tsmd_refit_load](figures/tsmd_refit_load.png)

### `refit_house1_load`

![refit_house1_load](figures/refit_house1_load.png)
