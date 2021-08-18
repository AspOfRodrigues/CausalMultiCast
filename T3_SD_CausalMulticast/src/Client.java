import CausalMulticast.CausalMulticast;
import CausalMulticast.ICausalMulticast;

import java.util.Scanner;

public class Client implements ICausalMulticast {
    private CausalMulticast cm;

    public Client(String ip, int port) throws Exception {
        cm = new CausalMulticast(ip, port, this);
    }

    public void chatApp() {
        while (true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Escreva sua mensagem: ");
            String msg = scanner.nextLine();
            cm.mcsend(msg,this);
        }
    }


    @Override
    public void deliver(String msg) {
        System.out.println("Mensagem chegou: " + msg);
    }


    public static void main(String args[]) throws Exception {
        String info;
        System.out.println("Digite o IP:PORTA");
        Scanner scanner = new Scanner(System.in);
        info = scanner.nextLine();
        String ip = info.split(":")[0];
        String port = info.split(":")[1];
        Client client = new Client(ip, Integer.decode(port));
        client.chatApp();
    }

}
