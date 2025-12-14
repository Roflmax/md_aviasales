# GitHub Actions Auto-Deploy Setup

## Шаги настройки:

### 1. Добавить секреты в GitHub

Перейти: https://github.com/Roflmax/md_aviasales/settings/secrets/actions

Нажать **New repository secret** для каждого:

| Name | Value |
|------|-------|
| `DEPLOY_HOST` | `5.129.201.78` |
| `DEPLOY_USER` | `root` |
| `DEPLOY_PATH` | `/root/md_aviasales` |
| `DEPLOY_SSH_KEY` | *(см. приватный ключ ниже)* |

### 2. Приватный SSH ключ

Скопировать **ВЕСЬ** текст ниже в секрет `DEPLOY_SSH_KEY`:

```
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAjAO/FKXxu03daA74ZKjqi/BFyvi2tv1E0/mxWuQX8NwAAAJieJ6sonier
KAAAAAtzc2gtZWQyNTUxOQAAACAjAO/FKXxu03daA74ZKjqi/BFyvi2tv1E0/mxWuQX8Nw
AAAEA7l9LkY8tOGW4TdQ5T4yhYYY5tISMzpKNjQL+alOnKACMA78UpfG7Td1oDvhkqOqL8
EXK+La2/UTT+bFa5Bfw3AAAAFWdpdGh1Yi1hY3Rpb25zLWRlcGxveQ==
-----END OPENSSH PRIVATE KEY-----
```

### 3. Готово!

После добавления секретов:
- Push в `main` → автоматический деплой
- Логи деплоя: https://github.com/Roflmax/md_aviasales/actions

## Как работает:

```
1. git push origin main
2. GitHub Actions запускается
3. SSH на сервер 5.129.201.78
4. cd /root/md_aviasales
5. git pull origin main
6. docker-compose down
7. docker-compose up -d --build
8. ✅ Деплой завершён
```

## Проверка работы:

После первого push в main:
- Открыть https://github.com/Roflmax/md_aviasales/actions
- Увидеть запущенный workflow "Deploy to Server"
- Проверить логи деплоя
