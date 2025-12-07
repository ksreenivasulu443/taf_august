import pytest
import sys

def main():
    # Customize the arguments for pytest run
    pytest_args = [
        "tests/nyc_taxi_load/s3_bronze_layer",
        "-v",
        "-s",
        "-m", "regression",
        "-k", "count"
    ]

    # Exit with pytest's exit code
    sys.exit(pytest.main(pytest_args))
#hsjfhsd
#updated in local

if __name__ == "__main__":
    main()
