# Despliegue de DFSha en AWS Academy (IaaS)

El enunciado pide (sección 5): VMs en AWS Academy o GCP, cada nodo nativo o
en un contenedor Docker, contenedores en máquinas distintas comunicándose
por puertos expuestos, y cada nodo exponiendo una API. Este es el
despliegue de referencia: **5 VMs, un nodo por VM, todos en Docker.**

| VM | Nodo | Puertos de entrada |
|---|---|---|
| vm-cn1 | ControlNode primario | 5000 (API REST HTTPS) |
| vm-cn2 | ControlNode standby | 5000 (API REST HTTPS) |
| vm-dn1 | DataNode 1 | 6001 (API de bloques TLS), 7001 (API HTTPS `/health`) |
| vm-dn2 | DataNode 2 | 6001, 7001 |
| vm-dn3 | DataNode 3 | 6001, 7001 |

Con pocos créditos se pueden juntar nodos: por ejemplo, 3 VMs con
ControlNode + DataNode en cada una (usando `DN_PORT` distintos). Lo
importante para la demo es que haya nodos en máquinas diferentes.

## 1. Crear las VMs

1. En AWS Academy, abre el *Learner Lab* y entra a la consola de EC2.
2. Lanza 5 instancias **Ubuntu 24.04**, tipo `t2.micro` o `t3.small`,
   con el mismo *key pair*.
3. Asigna una **Elastic IP** a cada una. Sin esto, la IP pública cambia al
   reiniciar el lab y se rompen el certificado y la configuración.
4. Crea un **Security Group** `dfsha-sg` y asígnalo a todas las VMs:

| Tipo | Puerto | Origen | Para qué |
|---|---|---|---|
| SSH | 22 | Mi IP | Administrar |
| TCP | 5000 | 0.0.0.0/0 | Cliente → ControlNode, DataNode → ControlNode, CN ↔ CN |
| TCP | 6001-6003 | 0.0.0.0/0 | Cliente → DataNode, CN → DN, DN ↔ DN |
| TCP | 7001-7003 | 0.0.0.0/0 | API `/health` de los DataNodes |

Todo el tráfico va cifrado con TLS, y las operaciones internas exigen
`CLUSTER_SECRET`, así que exponer los puertos es seguro para la demo.

## 2. Instalar Docker y el proyecto (en cada VM)

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git golang-go
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/sara-ivasquez/sistema-de-archivos-distribuidos.git
cd sistema-de-archivos-distribuidos
```

## 3. Generar el certificado UNA vez y copiarlo a todas las VMs

El certificado debe incluir las IPs con las que se contacta cada nodo. En
**una** VM (o en tu PC), con las 5 Elastic IPs:

```bash
cd gencert
DFSHA_CERT_HOSTS="IP_CN1,IP_CN2,IP_DN1,IP_DN2,IP_DN3" go run main.go
cd ..
```

Copia la carpeta `certs/` a las demás VMs (y al PC del cliente), por
ejemplo con `scp -r certs ubuntu@IP:~/sistema-de-archivos-distribuidos/`.
**Todas** deben usar el mismo `cert.pem`/`key.pem`. La carpeta está en
`.gitignore`: nunca la subas al repositorio.

## 4. Configurar y levantar cada nodo

En cada VM:

```bash
cp docker/aws/nodo.env.example docker/aws/.env
nano docker/aws/.env     # mismo CLUSTER_SECRET y DN_ENCRYPTION_KEY en todas
```

**vm-cn1** (`CN_ROLE=primary`, `CN_PEER_URL=https://IP_CN2:5000`):
```bash
docker compose -f docker/aws/controlnode.compose.yml --env-file docker/aws/.env up --build -d
```

**vm-cn2** (`CN_ROLE=standby`, `CN_PEER_URL=https://IP_CN1:5000`): el
mismo comando.

**vm-dn1..3** (`DN_PUBLIC_HOST=<Elastic IP de esa VM>`,
`CONTROLNODE_URLS=https://IP_CN1:5000,https://IP_CN2:5000`):
```bash
docker compose -f docker/aws/datanode.compose.yml --env-file docker/aws/.env up --build -d
```

Ver los logs: `docker compose -f docker/aws/<archivo>.yml logs -f`.

## 5. Usar el cliente (desde tu PC)

```bash
pip install -r requirements.txt
export DFSHA_CONTROLNODE_URL="https://IP_CN1:5000,https://IP_CN2:5000"
export DFSHA_CA_CERT=./certs/cert.pem
python client/cli.py register alice clave123
python client/cli.py login alice clave123
python client/cli.py put ./video.mp4 /video.mp4
```

En Windows (PowerShell): `$env:DFSHA_CONTROLNODE_URL="https://IP_CN1:5000,https://IP_CN2:5000"`.

## 6. Guion sugerido para el video

1. Mostrar las 5 VMs en la consola de EC2 y `docker ps` en cada una.
2. `register`/`login`, `mkdir`, `cd`, `put` de un archivo de varios MB.
   Mostrar en los logs de los DataNodes cómo llegan los bloques a distintas VMs.
3. `curl --cacert certs/cert.pem https://IP_DN1:7001/health` (API del DataNode).
4. **Caída de DataNode**: `docker stop` en vm-dn1. A los ~25 s, el log del
   ControlNode muestra la re-replicación. Hacer `get`: el archivo llega completo.
5. **Caída del ControlNode**: `docker stop` en vm-cn1. A los ~10 s, cn2 se
   promueve. Hacer `ls` y `get` **sin volver a hacer login**.
6. **Recuperación**: `docker start` en vm-cn1. Su log dice "arranco como
   STANDBY" y el estado sigue intacto.
7. **Seguridad**: login de un segundo usuario que no ve los archivos del
   primero. Mostrar un bloque en el disco del DataNode (está cifrado).
8. **Escalabilidad / rebalanceo**: lanzar una 6.ª VM con un DataNode nuevo
   (`docker compose -f docker/aws/datanode.compose.yml ... up -d`). En ~20 s,
   el log del ControlNode muestra "rebalanceo: N bloque(s) movidos". En
   `/health` de cada DataNode, `num_blocks` queda parejo. (Recuerda incluir
   su IP al generar el certificado.)
9. **RF3 y concurrencia**: abrir dos terminales con `python client/cli.py shell`.
   - En la primera: `open /notas.txt rw create` → `write 3 hola`. Aún no hagas `close`.
   - En la segunda: `open /notas.txt rw` → "bloqueado por otro proceso".
   - En la segunda: `open /notas.txt r` → `lock 3 shared` → también bloqueado.
   - En la primera: `close 3`. En la segunda, `lock 3 shared` ahora sí funciona.
   - Para mostrar la lectura parcial: `cat /archivo_grande --offset 5000000 --length 100`.
     Los logs de los DataNodes muestran que solo se pidió un bloque.
