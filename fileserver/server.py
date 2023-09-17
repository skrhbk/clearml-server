from config import config
log = config.logger(__file__)

storage = config.get("fileserver.storage")
log.info(f"Start fileserver with storage: {storage}")
if storage == "filesytem":
    import fileserver
    fileserver.main()
elif storage == "minio":
    import fileserver_minio
    fileserver_minio.main()
else:
    log.error(f"Unsupported fileserver.storage: {storage}")
    exit(-1)
