if [ -z "$1" ]
  then
    echo "No argument supplied, requires build version"
    exit 1
fi

set -euo pipefail

distro=$1
path=`pwd`
echo "About to launch $distro container"
container="skynet-build-$RANDOM"

#function finish {
#    echo "Cleaning up: ($?)!"
#    docker kill ${container}
#	sleep 5
#    docker rm ${container}
#    echo "finished cleaning up"
#}
#trap finish EXIT

echo "Named container: ${container}"
docker run --name ${container} -d -i -t -v $path:/build -w /build $distro
echo "Launched ${container}"

echo "Installing deps"
if [[ "$distro" == centos* ]]
    then
	docker exec ${container} yum update -y
	echo "installing"
    packages="libatasmart-devel openssl-devel protobuf-compiler protobuf-devel librados2-devel"
	docker exec ${container} yum install -y $packages
fi

if [[ "$distro" == ubuntu* ]]
    then
	docker exec ${container} apt update
	echo "installing "
  packages="clang curl libapt-pkg-dev libprotobuf-dev librados-dev libzmq3-dev pkg-config protobuf-compiler"
	docker exec ${container} apt-get install -y $packages
fi

echo "About to install rust"
docker exec ${container} curl https://sh.rustup.rs -o /root/rustup.sh
echo "chmod"
docker exec ${container} chmod +x /root/rustup.sh
echo "installing rust"
docker exec ${container} /root/rustup.sh -y

echo "Building"
docker exec ${container} /root/.cargo/bin/cargo build --release --all

echo "Release directory"
ls $path/target/release/
docker exec ${container} mv target/release/skynet target/release/bynar-$distro

#finish
