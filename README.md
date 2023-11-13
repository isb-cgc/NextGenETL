# NextGenETL: ETL Scripts

A set of scripts for ingesting data from CRDC cloud resources and other data sources.

## VM Set Up scripts

These are in `./scripts`:

- `setEnvVars.sh`: Needs to be edited to match your set-up. The running scripts pull configuration files
out of Google Cloud Storage. These two variables say where in GCS to find these files.

- `super_setup_vm.sh`: All scripts *must* be run on a Google cloud VM. This script will configure
the VM. Chicken-and-egg: though this script is in GitHub, you need to run it on the VM to get
GitHub support and download of this repo on the VM. Copy and paste the raw code.

- `add_python3_9.sh`: This script is needed if the VM does not have Python3.9 or greater installed on the machine.

- `reload-from-github.sh`: If you make code changes to the repo, run this script on the VM to pull changes
in. Dumb script just deletes the tree and clones the repo.
