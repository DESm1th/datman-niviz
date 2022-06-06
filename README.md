This is a datman/dashboard-ized version of Jer's lovely [niviz-rater app.](https://github.com/jerdra/niviz-rater)

## Installation
1. [install the dashboard](http://imaging-genetics.camh.ca/datman-dashboard/installation.html) if you haven't already. Note that the below instructions are for users following the 'full installation' instructions.
2. Add datman_niviz as a git submodule
```bash
cd datman-dashboard/dashboard/blueprints
git submodule add git@github.com:TIGRLab/datman_niviz.git
```
3. Install the requirements
```bash
pip install -r datman_niviz/niviz_rater/requirements.txt
```
4. Build the static files. Note that you must have npm installed. If this
step is successful it will have created the folder
`datman-dashboard/dashboard/static/niviz_rater`.
```bash
cd niviz_rater/static_src
npm install
npm run build
```
5. Configure your shell's paths. Update the `PYTHONPATH`.
```bash
export PYTHONPATH=${your_dash_install_path}/dashboard/blueprints/datman_niviz:${PYTHONPATH}
```
Update the `PATH`.
```bash
export PATH=${your_dash_install_path}/dashboard/blueprints/datman_niviz/niviz_rater/bin:${PATH}
```
6. Provide the full path to the configuration file in the
  `NIVIZ_RATER_CONF` environment variable. See the 'Configuration file' section
  for more info.
  ```bash
  export NIVIZ_RATER_CONF=$your_config_file_path
  ```
7. Initialize the databases. You must run this as the user that
the dashboard will use to access the database.
```bash
init_db.py
```

## Configuration File
For the dashboard to host niviz-rater, a configuration file must be provided
using an environment variable named `NIVIZ_RATER_CONF`. This file
must be yaml format and must contain one entry per pipeline.

Each entry must be formatted like below:
```YAML
STUDY_pipeline:
  base_dir: /some/path/to/niviz_rater/data
  qc_spec: /some/path/to/niviz_rater/spec_file.yaml
  bids_config: /some/path/to/alt_config.json          # bids_config is optional
  schema: /some/path/to/alt/schema.yaml               # schema is optional
```

`STUDY_pipeline` is the name to use when accessing the data through
    niviz-rater, and the name that will be used for the database.
  - 'STUDY' must be the name of a datman managed study.
  - 'pipeline' must be a unique name for the pipeline data to be displayed
    by niviz-rater
  - Note that 'STUDY' and 'pipeline' MUST be separated by an underscore.

At a minimum you must specify the `base_dir` and `qc_spec` arguments for
each entry. Any additional options you wish to pass to niviz-rater may be
included.
