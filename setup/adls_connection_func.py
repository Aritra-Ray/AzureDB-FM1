# Databricks notebook source
storage_name= dbutils.widgets.get("storage_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

def adls_conn_mount(storage_name, container_name):

    application_id = dbutils.secrets.get(scope="dbcourseappid", key="applicationid")
    tenant_id = dbutils.secrets.get(scope="dbcoursetenantid", key="tenantid")
    oauth_secrect = dbutils.secrets.get(scope="dbcoursesecret", key="dbudemysecret")
    print(application_id,tenant_id,oauth_secrect)
    if f"/mnt/{storage_name}/{container_name}"  in [mount.mountPoint for mount in dbutils.fs.mounts()]:
        dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")
    configs = {
            'fs.azure.account.auth.type': 'OAuth',
            'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
            'fs.azure.account.oauth2.client.id': application_id,
            'fs.azure.account.oauth2.client.secret': oauth_secrect,
            'fs.azure.account.oauth2.client.endpoint': f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
            }
    
    dbutils.fs.mount    (
            source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_name}/{container_name}",
            extra_configs =configs)
    return dbutils.notebook.exit({"Value": "success"}
                                 )


    

# COMMAND ----------

####run commands
adls_conn_mount(storage_name, container_name)
