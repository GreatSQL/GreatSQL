#
# Check if server has support for loading plugins
#

--disable_query_log
--disable_warnings
--let $turbo_install_require= 0
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip Requires dynamic loading
}

--let $turbo_plugin_installed=`select count(*) from information_schema.plugins where PLUGIN_NAME like 'turbo'`

if (!$turbo_plugin_installed)
{
  INSTALL PLUGIN turbo SONAME 'turbo.so';
  --let $turbo_install_require= 1
}

--enable_warnings
--enable_query_log
