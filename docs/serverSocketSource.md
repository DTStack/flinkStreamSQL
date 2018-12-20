
## 1.数据格式：
```
数据现在只支持json格式   {"xx":"bb","cc":"dd"}

CREATE TABLE MyTable(
    channel varchar,
    pv int,
    xctime date,
	xtime date
    
 )WITH(	
	type='serversocket',
	host='127.0.0.1',
	port='8888',
	delimiter=';',
	maxNumRetries='100'
 );
```


## 2.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | serversocket | 是||
|host | server host|是||
|port | server port|是||
|delimiter| 每条json数据的分割符(比如：;)|是||
|maxNumRetries| 最大重连次数 (大于0)|是||


## 3.Server端样例：
```
String str = "{\"CHANNEL\":\"xc3\",\"pv\":1234567,\"xdate\":\"2018-12-07\",\"xtime\":\"2018-12-15\"};";


public class TimeServerHandler implements Runnable {
	Socket socket;

	String str = "{\"CHANNEL\":\"xc3\",\"pv\":1234567,\"xdate\":\"2018-12-07\",\"xtime\":\"2018-12-15\"};";

	public TimeServerHandler(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		PrintWriter out = null;
		try {
			out = new PrintWriter(this.socket.getOutputStream(), true);
			while (true) {
				Thread.sleep(3000);
				out.println(str);
			}
		} catch (IOException e) {
			e.printStackTrace();

			if (out != null) {
				out.close();
			}
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
```
