netstat -tlnp |grep python3
lsof -i :44803|grep -v "PID"|awk '{print "kill -9",$2}'|sh
lsof -i :51500|grep -v "PID"|awk '{print "kill -9",$2}'|sh
lsof -i :51501|grep -v "PID"|awk '{print "kill -9",$2}'|sh
netstat -tlnp |grep python3
