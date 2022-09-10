cd /app/broadway_session_manager

perl init_upgrade_database.pl

perl broadway_proxy.pl &
sleep 1

perl auth_service.pl &
sleep 1

perl user_app_service.pl &
sleep 1

echo "Services started ..."

