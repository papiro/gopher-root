env "sqlite" {
  src = "file://pipeline/sqlite/schema.sql"
  url = getenv("DATABASE_URL")
  dev = "sqlite://dev?mode=memory"

  migration {
    dir = "file://pipeline/sqlite/migrations"
  }

  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
