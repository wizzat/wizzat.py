#!/bin/bash
echo "
    drop database wizzatpy_testdb;
    drop user wizzat;
    create user wizzat with createdb password 'wizzat';
    create database wizzatpy_testdb with owner wizzat;
" | sudo -u postgres psql


