package CausalMulticast;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.*;
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

    private int processId;
    private HashMap<Process, Integer> processList;
    private ICausalMulticast client;
    private DatagramSocket socket;

    private ArrayList<Message> buffer = new ArrayList<>();
    private Integer[][] matrixClock = new Integer[MAX_NUMBER_PROCESS][MAX_NUMBER_PROCESS];

    private ArrayList<Integer> disabledProcesses = new ArrayList<Integer>();
    private ArrayList<Integer> enabledProcesses = new ArrayList<Integer>();

    private void disableProcess(int id) {
        System.out.println("Desativando processo: " + id);
        disabledProcesses.add(id);
        enabledProcesses.remove((Object) id);
    }

    private void updateDelayedProcess() {
        System.out.println("Trocando lista de processos ativos com desativados");
        var temp = disabledProcesses;
        disabledProcesses = enabledProcesses;
        enabledProcesses = temp;
    }

    private void resetDisabledProcess() {
        System.out.println("Resetando lista de processos desativados");
        disabledProcesses.clear();
        for (var process : processList.entrySet()) {
            enabledProcesses.add(process.getValue());
        }
    }

    public CausalMulticast(String ip, Integer port, ICausalMulticast client) throws Exception {
        this.processList = new HashMap<>();
        this.client = client;
        this.socket = new DatagramSocket(port);

        for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
            for (int j = 0; j < MAX_NUMBER_PROCESS; j++) {
                matrixClock[i][j] = 0;
            }
        }

        discover(ip, port);
        resetDisabledProcess();
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
        Process currentProcess = new Process(ip, port);
        temporaryProcess.add(currentProcess);
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
                String members[] = data.replace("GROUP", "").replace("[", "").
                        replace("]", "").split(", ");
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
        if (!checkTimeout(s)) {
            System.out.println("Algum processo não recebeu tudo, enviando informações do grupo atual");
            msg = "GROUP" + temporaryProcess.toString();
            DatagramPacket discoverMsg = new DatagramPacket(msg.getBytes(), msg.length(),
                    group, GROUP_PORT);
            s.send(discoverMsg);
        }

        s.leaveGroup(group);
        this.processId = processList.get(currentProcess);
        processList.remove(currentProcess);
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

    Scanner scanner = new Scanner(System.in);

    private void queryForDelayedMessage(String msg) {
        Message message = new Message(msg, processId, matrixClock[processId]);
        printMatrixClock();
        matrixClock[processId][processId] += 1;

        System.out.println("Você deseja enviar para todos processos? 1 - Sim, 2 - Não");
        int sendAll = scanner.nextInt();
        if (sendAll != 1) {
            {
                for (var process : processList.entrySet()) {
                    System.out.println("Você deseja enviar pro processo " + process.getKey() + " ? 1 - Sim, 2 - Não");
                    int sendProcess = scanner.nextInt();
                    if (sendProcess != 1) {
                        disableProcess(process.getValue());
                    }
                }
                sendMessageToGroup(message);
                System.out.println("Enviar aos processos faltantes? 1 - Sim, 2 - Não");
                int sendProcess = scanner.nextInt();
                if (sendProcess == 1) {
                    updateDelayedProcess();
                    sendMessageToGroup(message);
                }

                resetDisabledProcess();
            }
        } else {
            sendMessageToGroup(message);
        }
    }

    private void sendMessageToGroup(Message message) {

        for (var processTuple : processList.entrySet()) {
            int processId = processTuple.getValue();
            Process process = processTuple.getKey();

            if (disabledProcesses.contains(processId)) continue;

            InetAddress processAddress = null;
            try {
                processAddress = InetAddress.getByName(process.ip);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            var serializedMessage = message.serialize();
            DatagramPacket udpPacket = new DatagramPacket(serializedMessage, serializedMessage.length,
                    processAddress, process.port);
            try {
                socket.send(udpPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void mcsend(String msg, Object client) {
        queryForDelayedMessage(msg);
    }


    private static class Message implements Serializable {
        String message;
        int processId;
        Integer[] vectorClock;
        boolean delivered = false;

        public Message(String message, int processId, Integer[] vectorClock) {
            this.message = message;
            this.processId = processId;
            this.vectorClock = vectorClock.clone();
        }

        public static Message deserialize(byte[] stream) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(stream);
            ObjectInputStream s = null;
            try {
                s = new ObjectInputStream(inputStream);
                return (Message) s.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        public byte[] serialize() {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ObjectOutputStream s = new ObjectOutputStream(outputStream);
                s.writeObject(this);
                return outputStream.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public String toString() {
            return "Message{" +
                    "message='" + message + '\'' +
                    ", processId=" + processId +
                    ", vectorClock=" + Arrays.toString(vectorClock) +
                    '}';
        }
    }

    private void printMatrixClock() {
        System.out.println("Current process: " + processId);
        System.out.println("Other process: " + processList);
        for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
            System.out.println(Arrays.toString(matrixClock[i]));
        }
    }

    private class Receiver implements Runnable {
        @Override
        public void run() {
            while (true) {
                byte[] buf = new byte[1000];
                DatagramPacket recv = new DatagramPacket(buf, buf.length);

                try {
                    socket.receive(recv);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Message message = Message.deserialize(recv.getData());
                matrixClock[message.processId] = message.vectorClock;

                printMatrixClock();
                buffer.add(message);
                buffer.sort((msg1, msg2) -> {
                    Integer[] vc1 = msg1.vectorClock;
                    Integer[] vc2 = msg2.vectorClock;
                    if(vc1 == vc2) return 0;
                    boolean less = true;
                    for (int i = 0; i < vc1.length; i++) {
                        less = less && vc1[i] <= vc2[i];
                    }
                    if (less) return -1;
                    else return 1;
                });

                if (processId != message.processId) matrixClock[processId][message.processId] += 1;

                //Delay message delivery
                for (Message item : buffer) {
                    System.out.println("_____BEGIN______");
                    System.out.println(item);
                    printMatrixClock();
                    System.out.println("_____END______");
                    if (item.delivered) continue;
                    boolean deliver = true;
                    for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
                        deliver = deliver && item.vectorClock[i] <= matrixClock[processId][i];
                    }
                    if (deliver) {
                        item.delivered = true;
                        client.deliver(item.message);
                    }
                }


                //Discard message from buffer
                for (int index = 0; index < buffer.size(); index++) {
                    Message item = buffer.get(index);
                    if (!item.delivered) continue;
                    int minCol = Integer.MAX_VALUE;
                    for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
                        if (matrixClock[i][item.processId] < minCol) minCol = matrixClock[i][item.processId];
                    }
                    if (item.vectorClock[item.processId] <= minCol) {
                        buffer.remove(item);
                        index--;
                    }
                }
                System.out.println("Buffer: " + buffer);
            }
        }
    }


}
