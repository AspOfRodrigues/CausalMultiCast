package CausalMulticast;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Executors;

public class CausalMulticast {

    private class Process {
        public String ip;
        public int port;

        public Process(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Process process = (Process) o;
            return port == process.port && Objects.equals(ip, process.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, port);
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }

    private static final String GROUP_IP = "228.5.6.7";
    private static final int GROUP_PORT = 6789;
    private static final int MAX_NUMBER_PROCESS = 3;
    private static final int TIMEOUT = 5000;

    private String ip;
    private int port;
    private HashMap<Process, Integer> processList;
    private ICausalMulticast client;
    private DatagramSocket socket;
    public CausalMulticast(String ip, Integer port, ICausalMulticast client) throws Exception {
        this.ip = ip;
        this.port = port;
        this.processList = new HashMap<>();
        this.client = client;
        this.socket = new DatagramSocket(port);
        discover(ip, port);

        var pool = Executors.newSingleThreadExecutor();
        pool.execute(new Receiver());
    }

    private boolean checkTimeout(MulticastSocket s) throws IOException {
        byte[] buf = new byte[1000];
        DatagramPacket recv = new DatagramPacket(buf, buf.length);
        s.setSoTimeout(4000);
        try {
            s.receive(recv);
            String data = new String(recv.getData(), recv.getOffset(), recv.getLength());
        } catch (SocketTimeoutException e) {
            System.out.println("No new message received");
            return true;
        }
        return false;
    }

    private void discover(String ip, int port) throws Exception {
        ArrayList<Process> temporaryProcess = new ArrayList<Process>();
        temporaryProcess.add(new Process(ip, port));
        MulticastSocket socket = new MulticastSocket();
        String msg = ip + ":" + port;
        InetAddress group = InetAddress.getByName(GROUP_IP);
        MulticastSocket s = new MulticastSocket(GROUP_PORT);
        s.joinGroup(group);
        while (temporaryProcess.size() < MAX_NUMBER_PROCESS) {
            DatagramPacket discoverMsg = new DatagramPacket(msg.getBytes(), msg.length(),
                    group, GROUP_PORT);
            s.send(discoverMsg);
            byte[] buf = new byte[1000];
            DatagramPacket recv = new DatagramPacket(buf, buf.length);

            s.receive(recv);

            String data = new String(recv.getData(), recv.getOffset(), recv.getLength());
            // If we receive the group info, we failed to receive some message before and need to override the info
            if (data.contains("GROUP")) {
                System.out.println("Grupo invalido, sobescrevendo informações locais com definição do grupo da rede");
                data = data.replace("GROUP", "");
                data = data.replace("[", "");
                data = data.replace("]", "");
                String members[] = data.split(", ");
                for (String member : members) {
                    addProcess(temporaryProcess, member);
                }
            } else {
                if (data.equals(msg)) continue;
                addProcess(temporaryProcess, data);
            }
        }

        temporaryProcess.sort(Comparator.comparing(Process::toString));

        for (int i = 0; i < temporaryProcess.size(); i++) {
            processList.put(temporaryProcess.get(i), i);
        }

        System.out.println(processList);

        System.out.println("Finalizou descobrimento, checando por processos que não receberam todo o grupo, e reenvia o grupo inteiro caso necessário");
        if(!checkTimeout(s)) {
            System.out.println("Algum processo não recebeu tudo, enviando informações do grupo atual");
            msg = "GROUP" + temporaryProcess.toString();
            DatagramPacket discoverMsg = new DatagramPacket(msg.getBytes(), msg.length(),
                    group, GROUP_PORT);
            s.send(discoverMsg);
        }

        s.leaveGroup(group);
    }

    private void addProcess(ArrayList<Process> temporaryProcess, String data) {
        String discover_ip = data.split(":")[0];
        String discover_port = data.split(":")[1];
        Process discoverProcess = new Process(discover_ip, Integer.decode(discover_port));
        if (!temporaryProcess.contains(discoverProcess)) {
            temporaryProcess.add(discoverProcess);
            System.out.println("Descobri processo:" + discoverProcess.toString());
        }
    }

    public void mcsend(String msg, Object client) {
        for(var processTuple : processList.entrySet()){
            int processId = processTuple.getValue();
            Process process = processTuple.getKey();
            InetAddress processAddress = null;
            try {
                processAddress = InetAddress.getByName(process.ip);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            DatagramPacket message = new DatagramPacket(msg.getBytes(), msg.length(),
                    processAddress, process.port);
            try {
                socket.send(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static class Receiver implements Runnable {

        @Override
        public void run() {

        }
    }


}
