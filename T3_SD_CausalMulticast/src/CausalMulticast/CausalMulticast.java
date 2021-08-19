package CausalMulticast;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;

public class CausalMulticast {
    /**
     * Classe que guardo o ip e porta de um processo
     */
    private class Process {
        public String ip;
        public int port;

        /**
         * Construtor da classe processo
         *
         * @param ip   ip passado por parametro na Classe cliente
         * @param port porta passada por parametro na Classe cliente
         */
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


    private int processId;
    private HashMap<Process, Integer> processList;
    private ICausalMulticast client;
    private DatagramSocket socket;


    private ArrayList<Message> buffer = new ArrayList<>();

    private Integer[][] matrixClock = new Integer[MAX_NUMBER_PROCESS][MAX_NUMBER_PROCESS];

    private ArrayList<Integer> disabledProcesses = new ArrayList<Integer>();
    private ArrayList<Integer> enabledProcesses = new ArrayList<Integer>();
    Scanner scanner = new Scanner(System.in);

    /**
     * Construtor da classe, atribui nas propriedades os valores passados por parametro e registra o processo no metodo
     * discover
     * todas as posicoes do matrix clock de cada processo sao iniciadas com valor -1
     *
     * @param ip     ip passado por parametro para o registro do processo
     * @param port   porta passada por parametro para o registro do processo
     * @param client proprio cliente passado por parametro
     * @throws Exception
     */
    public CausalMulticast(String ip, Integer port, ICausalMulticast client) throws Exception {
        this.processList = new HashMap<>();
        this.client = client;
        this.socket = new DatagramSocket(port);


        discover(ip, port);
        resetDisabledProcess();

        for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
            for (int j = 0; j < MAX_NUMBER_PROCESS; j++) {
                if (i == j && i == processId)
                    matrixClock[i][j] = 0;
                else
                    matrixClock[i][j] = -1;
            }
        }
        printMatrixClock();

        var pool = Executors.newSingleThreadExecutor();
        pool.execute(new Receiver());
    }

    /**
     * Basicamente cria um MulticastSocket e faz o registro no grupo, enquanto o numero de processo maximo
     * nao for atigido, este processo vai enviar um Datagram Packet para descobrir novos processos, caso a mesagem
     * retornada for igual ,significa que este processo sera registrado
     *
     * @param ip   ip passado por parametro para o registro do processo
     * @param port porta passada por parametro para o registro do processo
     * @throws Exception
     */
    private void discover(String ip, int port) throws Exception {
        ArrayList<Process> temporaryProcess = new ArrayList<Process>();
        Process currentProcess = new Process(ip, port);
        temporaryProcess.add(currentProcess);
        String msg = ip + ":" + port;
        InetAddress group = InetAddress.getByName(GROUP_IP);
        MulticastSocket s = new MulticastSocket(GROUP_PORT);
        s.joinGroup(group);

        DatagramPacket discoverMsg = new DatagramPacket(msg.getBytes(), msg.length(),
                group, GROUP_PORT);
        while (temporaryProcess.size() < MAX_NUMBER_PROCESS) {
            s.send(discoverMsg);
            byte[] buf = new byte[1000];
            DatagramPacket recv = new DatagramPacket(buf, buf.length);

            s.receive(recv);
            String data = new String(recv.getData(), recv.getOffset(), recv.getLength());
            if (data.equals(msg)) continue;
            addProcess(temporaryProcess, data);
        }

        System.out.println("Grupo descoberto, enviando últimas mensagens para certificar que todos membros tem a lista completa");
        long time = System.currentTimeMillis();
        long elapsedTime = 0;
        int timeout = 2000;
        while (elapsedTime < timeout) {
            s.send(discoverMsg);
            elapsedTime = System.currentTimeMillis() - time;
        }

        temporaryProcess.sort(Comparator.comparing(Process::toString));

        for (int i = 0; i < temporaryProcess.size(); i++) {
            processList.put(temporaryProcess.get(i), i);
        }

        System.out.println(processList);

        s.leaveGroup(group);
        this.processId = processList.get(currentProcess);
    }

    /**
     * Desabilita que um processo possa receber a mensagem
     *
     * @param id id do processo a ser desativado
     */
    private void disableProcess(int id) {
        System.out.println("Desativando processo: " + id);
        disabledProcesses.add(id);
        enabledProcesses.remove((Object) id);
    }

    /**
     * Sincroniza processos que ainda nao receberam a mensagem, e remove aqueles que ja receberam, evitando enviar 2
     * vezes a mensagem para um processo que já recebeu
     */
    private void updateDelayedProcess() {
        System.out.println("Trocando lista de processos ativos com desativados");
        var temp = disabledProcesses;
        disabledProcesses = enabledProcesses;
        enabledProcesses = temp;
    }

    /**
     * Reseta a lista de processos desativados, a partir disso todos passam a poder receber mensagens
     */
    private void resetDisabledProcess() {
        System.out.println("Resetando lista de processos desativados");
        disabledProcesses.clear();
        for (var process : processList.entrySet()) {
            enabledProcesses.add(process.getValue());
        }
    }

    private void addProcess(ArrayList<Process> processList, String data) {
        String discover_ip = data.split(":")[0];
        String discover_port = data.split(":")[1];
        Process discoverProcess = new Process(discover_ip, Integer.decode(discover_port));
        if (!processList.contains(discoverProcess)) {
            processList.add(discoverProcess);
            System.out.println("Descobri processo:" + discoverProcess.toString());
        }
    }

    /**
     * Faz o controle do envio de mensagens, basicamente decide conforme o input do usuario se a mensagem passada por
     * parametro deve ser enviada a todos os processos, ou seletivamente, questionando qual processo enviar
     *
     * @param msg mensagem a ser enviada aos demais processos
     */
    private void queryForDelayedMessage(String msg) {
        Message message = new Message(msg, processId, matrixClock[processId]);
        System.out.println("Piggyback vector clock: " + Arrays.toString(matrixClock[processId]));
        matrixClock[processId][processId] += 1;
        printMatrixClock();


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

    /**
     * Envia a mensagem serializada passada por parametro aos demais processos, por meio de um datagram
     * packet, verificando se o processo em questao a ser enviado, seria um processo habilitado a receber
     * aquela mensagem
     * Simula o comportamento de um multicast usando unicast
     *
     * @param message mensagem a ser enviada
     */
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

    /**
     * Envia a mensagem ao grupo multicast criado na fase de descobrimento, gerencia o matrix clock para possibilitar ordenação causal e estabilização de mensagens
     *
     * @param msg    mensagem a ser enviada
     * @param client
     */
    public void mcsend(String msg, Object client) {
        queryForDelayedMessage(msg);
    }

    /**
     * Funcao auxiliar para imprimir o matrix clock
     */
    private void printMatrixClock() {
        System.out.println("Current process: " + processId);
        System.out.println("Other process: " + processList);
        for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
            System.out.println(Arrays.toString(matrixClock[i]));
        }
    }

    /**
     * Classe que implementa uma mensagem que sera enviada por algum processo
     */
    private static class Message implements Serializable {
        String message;
        int processId;
        Integer[] vectorClock;
        boolean delivered = false;

        /**
         * Construtor da classe
         *
         * @param message     a mensagem em si a ser entregue
         * @param processId   o id do processo que enviou a mensagem
         * @param vectorClock vector clock do processo que enviou a mensagem
         */
        public Message(String message, int processId, Integer[] vectorClock) {
            this.message = message;
            this.processId = processId;
            this.vectorClock = vectorClock.clone();
        }

        /**
         * Desserializa e retorna um objeto mensagem
         *
         * @param stream
         * @return
         */
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

        /**
         * @return Serializa e retorna a mensagem serializada
         */
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

        /**
         * @return informacoes da mensagem formatadas
         */
        @Override
        public String toString() {
            return "Message{" +
                    "message='" + message + '\'' +
                    ", processId=" + processId +
                    ", vectorClock=" + Arrays.toString(vectorClock) +
                    '}';
        }
    }

    /**
     * Classe que instancia uma thread para o recebimento de mensagens assicrono
     */
    private class Receiver implements Runnable {
        @Override
        /**
         *  Implementa o Controle de recebimento da mensagem
         *  A mensagem vai ser recebida e desserializada, entao vai ser verificado a partir do Vector Clock se a
         *  mensagem pode ser recebida pelo processo. Se todos processos receberem a mensagem, ela sera descartada
         *  do buffer, fazendo a estabilização de mensagens
         */
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
                if (processId != message.processId) matrixClock[message.processId] = message.vectorClock;

                buffer.add(message);
                buffer.sort((msg1, msg2) -> {
                    Integer[] vc1 = msg1.vectorClock;
                    Integer[] vc2 = msg2.vectorClock;
                    if (vc1 == vc2) return 0;
                    int sum0 = 0;
                    int sum1 = 0;
                    for (int i = 0; i < vc1.length; i++) {
                        sum0 += vc1[i];
                        sum1 += vc2[i];
                    }
                    return Integer.compare(sum0, sum1);
                });

                System.out.println("Ordered buffer: " + buffer);
                if (processId != message.processId) matrixClock[processId][message.processId] += 1;

                //Delay message delivery - CAUSAL ORDER
                for (Message item : buffer) {
                    if (item.delivered) continue;
                    boolean deliver = true;
                    for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
                        deliver = deliver && item.vectorClock[i] <= matrixClock[processId][i];
                    }
                    if (deliver) {
                        System.out.println("MENSAGEM ENTREGUE: " + item);
                        item.delivered = true;
                        client.deliver(item.message);
                    }
                }


                //Discard message from buffer - MESSAGE STABILIZATION
                for (int index = 0; index < buffer.size(); index++) {
                    Message item = buffer.get(index);
                    if (!item.delivered) continue;
                    int minCol = Integer.MAX_VALUE;
                    for (int i = 0; i < MAX_NUMBER_PROCESS; i++) {
                        if (matrixClock[i][item.processId] < minCol) minCol = matrixClock[i][item.processId];
                    }
                    if (item.vectorClock[item.processId] <= minCol) {
                        System.out.println("MENSAGEM DESCARTADA: " + item);
                        buffer.remove(item);
                        index--;
                    }
                }
                printMatrixClock();
                System.out.println("Buffer: " + buffer);
            }
        }
    }


}
