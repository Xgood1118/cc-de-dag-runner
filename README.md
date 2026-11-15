# dag-runner

任务编排骨架。当前按列表顺序跑，遇到第一个失败就停。

## 已知缺口

- 不读 depends_on，完全按数组顺序
- 不检测环，E/F 互相依赖会被顺序执行一遍
- 没有 skipped 语义，失败后续任务直接不跑
- 不重试、不落状态、不支持 resume

## 运行

```powershell
python gen_samples.py
python -m dag.cli tasks.json
python -m pytest tests -q
```
