
--connection default
# Do not restore connection, because we have disconnected it.

if ($turbo_install_require)
{
--disable_query_log
--disable_warnings
UNINSTALL PLUGIN turbo;

--let $turbo_install_require= 0

--enable_warnings
--enable_query_log

}