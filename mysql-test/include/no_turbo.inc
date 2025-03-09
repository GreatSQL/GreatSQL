--disable_query_log
--disable_warnings
if (`select count(*)<> 0 from information_schema.plugins where PLUGIN_NAME like 'turbo'`) {
  let $have_turbo = `select @@turbo_enable`;
  if ($have_turbo)
  {
    --skip Test requires 'no_turbo'
  }
}
flush status;
--enable_warnings
--enable_query_log
