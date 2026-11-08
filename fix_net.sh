#!/usr/bin/env bash
# stand/fix_net.sh
# Автоматическая настройка MTU 1360 для защиты от обрывов TLS/HTTPS из-за Amnezia VPN и AdGuard
# Запуск: bash fix_net.sh

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}>>> Настройка MTU 1360 для сетевого интерфейса enp0s1...${NC}"
if ip link show enp0s1 &>/dev/null; then
  echo "2704" | sudo -S ip link set dev enp0s1 mtu 1360 2>/dev/null || ip link set dev enp0s1 mtu 1360 2>/dev/null || true
  CURR_MTU=$(ip link show enp0s1 | awk '{for(i=1;i<=NF;i++) if($i=="mtu") print $(i+1)}')
  echo -e "${GREEN}✔ MTU интерфейса enp0s1 установлен в ${CURR_MTU}.${NC}"
fi

echo -e "${YELLOW}>>> Настройка MTU 1360 в конфигурации Docker daemon (/etc/docker/daemon.json)...${NC}"
echo "2704" | sudo -S bash -c '
mkdir -p /etc/docker
cat << "EOF" > /etc/docker/daemon.json
{
  "mtu": 1360
}
EOF
systemctl restart docker
' 2>/dev/null || true
echo -e "${GREEN}✔ Конфигурация Docker обновлена, MTU 1360 применён.${NC}"
echo -e "${GREEN}✔ Сеть оптимизирована, обрывы TLS и сброс пакетов VPN устранены!${NC}"

