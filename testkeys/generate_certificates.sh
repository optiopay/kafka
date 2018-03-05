set -o xtrace

find . -type f ! -name '*.sh' ! -name '*.cnf' -delete
rm -rf ./newcerts/

#openssl genrsa -out example.org.key 2048
openssl genrsa -out example.org.key 2048
openssl req -new -key example.org.key -out example.org.csr -subj "/C=DE/ST=Berlin/L=Berlin/O=Optiopay/CN=api.optiopay.com" 
openssl req -new -out oats.csr -config oats.cnf
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -subj "/C=DE/ST=Berlin/L=Berlin/O=Optiopay/CN=optiopay.com"
mkdir newcerts
touch index.txt
echo '01' > serial
openssl ca -config ca.cnf -out example.org.crt -infiles example.org.csr
openssl ca -config ca.cnf -out oats.crt -extfile oats.extensions.cnf -in oats.csr
