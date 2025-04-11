import time
import dask
from dask.distributed import Client, LocalCluster
from dask_cloudprovider.aws import FargateCluster, EC2Cluster


def say_hello(name):
    """Function to say hello to a name with a delay"""
    time.sleep(1)  # Simulate some work
    return f"Hello, {name} from Dask on AWS EC2!"


def main():
    # Start a local Dask cluster
    cluster = LocalCluster()

    # Start an EC2 Dask cluster
    """cluster = EC2Cluster(
        n_workers=2,
        security=False,  # Avoid encountering this error: https://github.com/dask/dask-cloudprovider/issues/249
        region="us-east-1",
    )"""

    # Start a Fragate Dask cluster
    """cluster = FargateCluster(
        n_workers=2,
        region_name="us-east-1",
    )"""

    client = Client(cluster)

    print(f"Dask dashboard available at: {client.dashboard_link}")

    # Create a list of names
    names = ["World", "AWS", "EC2", "Dask", "Python"]

    # Create delayed tasks
    results = []
    for name in names:
        # Schedule the task to run on the cluster
        future = client.submit(say_hello, name)
        results.append(future)

    # Gather and print results
    print("\nResults:")
    for future in results:
        print(f"  {future.result()}")

    # Display cluster information
    print("\nCluster Information:")
    print(f"  Workers: {len(client.scheduler_info()['workers'])}")
    print(f"  Cores: {sum(w['nthreads'] for w in client.scheduler_info()['workers'].values())}")

    # Clean up
    try:
        client.close()
        cluster.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
