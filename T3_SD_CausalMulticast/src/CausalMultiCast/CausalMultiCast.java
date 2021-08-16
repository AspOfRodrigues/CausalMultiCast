package CausalMultiCast;

import java.awt.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;

public class CausalMultiCast {
    private class Process{
        public String ip;
        public int port;
        public Process(String ip, int port){
            this.ip = ip;
            this.port = port;
        }
    }
    private static final String GROUP_IP =  "228.5.6.7";
    private static final int GROUP_PORT = 6789;
    private static final int MAX_NUMBER_PROCESS = 3;
    private static final int TIMEOUT = 5000;

    private String ip;
    private int port;
    private HashMap<String,Process> processList;

    public CausalMultiCast(String ip, Integer port) throws Exception {
        this.ip = ip;
        this.port = port;
        this.processList = new HashMap<>();
        discover(ip, port);
    }

    private void discover(String ip, int port) throws Exception {
        MulticastSocket socket = new MulticastSocket();
        String msg = ip + ":" + port;
        InetAddress group = InetAddress.getByName(GROUP_IP);
        MulticastSocket s = new MulticastSocket(GROUP_PORT);
        s.joinGroup(group);
        while (processList.size() < MAX_NUMBER_PROCESS) {
            DatagramPacket discoverMsg = new DatagramPacket(msg.getBytes(), msg.length(),
                    group, GROUP_PORT);
            s.send(discoverMsg);
            byte[] buf = new byte[1000];
            DatagramPacket recv = new DatagramPacket(buf, buf.length);
            s.receive(recv);
            String data = new String(recv.getData(),recv.getOffset(),recv.getLength());
            if(data.equals(msg))continue;

            if(!processList.containsKey(data))
            {
                String discover_ip = data.split(":")[0];
                String discover_port = data.split(":")[1];
                processList.put(data,new Process(discover_ip, Integer.decode(discover_port)));
                System.out.println("Descobri processo:" + data);
            }
        }
        s.leaveGroup(group);
    }


    public void mcsend(String msg, Object client)
    {

    }

}
