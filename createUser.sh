#! /bin/bash

use admin

db.createUser({user: 'u', pwd: 'password', roles: [{role: "readWrite", db: "tdt4225"}]})