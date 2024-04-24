#!/bin/zsh

base_ip=$1
# home="127.0.0.1:5000"
app="localhost:5000"
lb="http://$base_ip.2:5000"
s0="http://$base_ip.3:5000"
s1="http://$base_ip.4:5000"
s2="http://$base_ip.5:5000"

# Initialise
curl -X POST "$app/init" -H "Content-Type: application/json" -d @"../load_balancer/init.json"

# Status
curl "$app/status"

# Add
curl -X POST "$app/add" -H "Content-Type: application/json" -d @"../load_balancer/add.json"

# Remove
curl -X DELETE "$app/rm" -H "Content-Type: application/json" -d @"../load_balancer/rm.json"

# Read
curl -X POST "$app/read" -H "Content-Type: application/json" -d @"../load_balancer/read.json"

# Write
curl -X POST "$app/write" -H "Content-Type: application/json" -d @"../load_balancer/write.json"

# Update
curl -X PUT "$app/update" -H "Content-Type: application/json" -d @"../load_balancer/update.json"

# Delete
curl -X PUT "$app/del" -H "Content-Type: application/json" -d @"../load_balancer/del.json"
