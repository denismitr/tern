package cli

const configFileStub = `version: "1"

paths:
  local_folder: "./migrations"
  database_url: "mysql://username:password@(127.0.0.1:3306)/your_db_name?parseTime=true"
  version_format: datetime
`
