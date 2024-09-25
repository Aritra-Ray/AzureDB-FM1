# Databricks notebook source
def adls_conn_mount(storage_name, container_name):
    ##get sas key secrets
    sas_key = dbutils.secrets.get(scope="dbudemycourse", key="sasdbcourse")
    ## if mount is not present create a new mount point
    if f"/mnt/{storage_name}/{container_name}" not in [mount.mountPoint for mount in dbutils.fs.mounts()]:
        dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_name}/{container_name}",
        extra_configs = {f"fs.azure.sas.{container_name}.{storage_name}.blob.core.windows.net": sas_key}
        )
    display(dbutils.fs.mounts())


    

# COMMAND ----------

##run commands
adls_conn_mount("dludemycourse", "demo")

# COMMAND ----------


