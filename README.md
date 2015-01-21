# ChainReplication
1) INSTRUCTIONS:
   There is a bash file called run in DistAlgoCode
   Run the bash file and pass the config file path as the command line argument
   For example,
   ./run 'config/config1.json'

2) MAIN FILES:
   DistAlgoCode has chainReplication.da which will read the config file and start the clients and the servers accordingly.
   DistAlgoCode/src/client/ has client.da which will spawn clients and handle message passing.
   DistAlgoCode/src/server/ has server.da which will spawn servers and handle message passing.
   DistAlgoCode/src/master/ has master.da which will spawn the master and handle message passing.

