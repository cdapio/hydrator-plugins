#!/bin/bash
while true;do
  echo -n ""
  read USERNAME
  if [[ -z $USERNAME ]];then
    break
  else
    echo "Welcome User, $USERNAME."
  fi
done
