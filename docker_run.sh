sudo docker stop alerts_test
sudo docker rm alerts_test
sudo docker build -t alerts_test .
sudo docker run -d --name alerts_test alerts_test
sudo docker logs -f alerts_test
