import re

import ml_scheduler

# at least 90% of the GPU memory should be free
cuda = ml_scheduler.pools.CUDAPool([0, 2], 90)
disk = ml_scheduler.pools.DiskPool('/one-fs')


@ml_scheduler.exp_func
async def mmlu(exp: ml_scheduler.Exp, model, checkpoint):
    """Copy the model from another filesystem and run inferences on one GPU."""

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

    async def report(m, stdout):
        # parse the metrics from the output
        # See https://github.com/RUCAIBox/LLMBox for more details
        parts = stdout.rsplit("#####", 1)
        metrics = re.search(r"\d+\.\d+", parts[1]).group()
        await exp.report({m: metrics})

    # run inference
    args = [
        "python", "inference.py", "--model", target_dir, "--dataset", "mmlu"
    ]
    env = {
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        "CUDA_VISIBLE_DEVICES": ",".join(map(str, cuda_resource)),
    }
    stdout = await exp.run(args=args, env=env)
    await report('Accuracy', stdout)

    # run another inference with the same resources
    args = [
        "python", "inference.py", "--model", target_dir, "--dataset", "mmlu",
        "--num_shots", "5"
    ]
    stdout = await exp.run(args=args, env=env)
    await report('Accuracy (5-shots)', stdout)


mmlu.run_csv(
    "experiments.csv",
    ['Accuracy', 'Accuracy (5-shots)'],
)
