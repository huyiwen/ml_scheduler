# ml_scheduler

[![PyPI version](https://badge.fury.io/py/ml_scheduler.svg)](http://badge.fury.io/py/ml_scheduler)
[![Test Status](https://github.com/huyiwen/ml_scheduler/workflows/Test/badge.svg?branch=develop)](https://github.com/huyiwen/ml_scheduler/actions?query=workflow%3ATest)
[![Lint Status](https://github.com/huyiwen/ml_scheduler/workflows/Lint/badge.svg?branch=develop)](https://github.com/huyiwen/ml_scheduler/actions?query=workflow%3ALint)
[![codecov](https://codecov.io/gh/huyiwen/ml_scheduler/branch/main/graph/badge.svg)](https://codecov.io/gh/huyiwen/ml_scheduler)
[![Join the chat at https://gitter.im/huyiwen/ml_scheduler](https://badges.gitter.im/huyiwen/ml_scheduler.svg)](https://gitter.im/huyiwen/ml_scheduler?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://pypi.python.org/pypi/ml_scheduler/)
[![Downloads](https://pepy.tech/badge/ml_scheduler)](https://pepy.tech/project/ml_scheduler)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://timothycrosley.github.io/isort/)
_________________

[Browse GitHub Code Repository](https://github.com/huyiwen/ml_scheduler/)
_________________

**ml_scheduler** A lightweight machine learning experiments scheduler in a few lines of simple Python

## Quick Start

1. Install ml_scheduler

```bash
pip install ml_scheduler
```

2. Create a Python script:

```python
cuda = ml_scheduler.pools.CUDAPool([0, 2], 90)
disk = ml_scheduler.pools.DiskPool('/one-fs')


@ml_scheduler.exp_func
async def mmlu(exp: ml_scheduler.Exp, model, checkpoint):

    source_dir = f"/another-fs/model/{model}/checkpoint-{checkpoint}"
    target_dir = f"/one-fs/model/{model}-{checkpoint}"

    # resources will be cleaned up after exiting the function
    disk_resource = await exp.get(
        disk.copy_folder,
        source_dir,
        target_dir,
        cleanup_target=True,
    )
    cuda_resource = await exp.get(cuda.allocate, 1)

    # run inference
    args = [
        "python", "inference.py", "--model", target_dir, "--dataset", "mmlu", "--cuda",  str(cuda_resource[0])
    ]
    stdout = await exp.run(args=args)
    await exp.report({'Accuracy', stdout})


mmlu.run_csv("experiments.csv", ['Accuracy'])
```

Mark the function with `@ml_scheduler.exp_func` and `async` to make it an experiment function. The function should take an `exp` argument as the first argument.

Then use `await exp.get` to get resources (non-blocking) and `await exp.run` to run the experiment (also non-blocking). Non-blocking means that when you can run multiple experiments concurrently.

3. Create a CSV file `experiments.csv` with your arguments (`model` and `checkpoint` in this case):

```csv
model,checkpoint
alpacaflan-packing,200
alpacaflan-packing,400
alpacaflan-qlora,200-merged
alpacaflan-qlora,400-merged
```

4. Run the script:

```bash
python run.py
```

The results (`Accuracy` in this case) and some other information will be saved in `results.csv`.
