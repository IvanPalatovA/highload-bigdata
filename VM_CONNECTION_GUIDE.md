# Инструкция для агента: Подключение к ВМ (Ubuntu ARM64) при активном VPN

> **Важно для AI-агента:**  
> На хосте (macOS) постоянно включен VPN (Amnezia / AdGuard) с активным Kill Switch. Из-за этого прямой вызов `ssh ivan@192.168.64.8` с мака падает с ошибкой:
> `ssh: connect to host 192.168.64.8 port 22: No route to host (Errno 65)`.
> 
> **Решение:** Подключаться через легковесный Docker-контейнер (`vm-ssh`). Виртуальная машина Docker Desktop имеет прямой сетевой мост в гипервизор к `192.168.64.8` в обход интерфейса VPN (`utun`).

---

## 1. Реквизиты стенда и ВМ
- **Хост ВМ:** `192.168.64.8`
- **Пользователь:** `ivan`
- **Пароль:** `2704`
- **SSH Key Passphrase:** пустой (Enter)
- **Рабочая директория репозитория на ВМ:** `/home/ivan/stand`

---

## 2. Быстрое подключение (Готовая команда)

### Шаг 1: Проверить / собрать образ `vm-ssh` (занимает 2 секунды)
Если образ `vm-ssh` ещё не собран на хосте:
```bash
docker build -t vm-ssh - << 'EOF'
FROM alpine:3.20
RUN apk add --no-cache openssh-client sshpass bash
ENTRYPOINT ["sshpass", "-p", "2704", "ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "ivan@192.168.64.8"]
EOF
```

### Шаг 2: Выполнить любую команду на ВМ
```bash
# Проверка связи
docker run --rm vm-ssh "uname -a"

# Обновить код на ВМ из GitHub
docker run --rm vm-ssh "cd ~/stand && git pull origin main"

# Запуск проверки Лабы 4
docker run --rm vm-ssh "cd ~/stand && bash check_lab4.sh"

# Запуск проверки Лабы 5
docker run --rm vm-ssh "cd ~/stand && bash check_lab5.sh"
```

---

## 3. Сетевые особенности внутри самой ВМ (MTU / Docker)
Если внутри ВМ Docker-контейнеры Spark / ClickHouse или `git pull` зависают или падают с `TLS handshake EOF`:
- На сетевом интерфейсе `enp0s1` и в Docker daemon ВМ должен быть установлен **MTU 1360** (из-за накладных расходов VPN-туннеля).
- Скрипт автоматического исправления на ВМ:
  ```bash
  docker run --rm vm-ssh "cd ~/stand && bash fix_net.sh"
  ```
  Или вручную:
  ```bash
  docker run --rm vm-ssh "sudo ip link set dev enp0s1 mtu 1360"
  ```

---

## 4. Памятка по рабочему процессу агента
1. Вносишь правки в код в локальной директории `stand/`.
2. Делаешь `git commit` и `git push origin main`.
3. Запускаешь на ВМ обновление и тесты через докер:
   ```bash
   docker run --rm vm-ssh "cd ~/stand && git pull origin main && bash check_lab4.sh"
   ```
4. Если вывод содержит ошибки — анализируешь, исправляешь локально, пушишь и повторяешь шаг 3.
