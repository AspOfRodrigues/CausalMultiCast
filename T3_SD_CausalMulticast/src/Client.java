import CausalMulticast.CausalMulticast;
import CausalMulticast.ICausalMulticast;
import java.util.Scanner;

public class Client implements ICausalMulticast {
    private CausalMulticast cm;
    public Client(String ip, int port) throws Exception {
        cm = new CausalMulticast(ip,port, this);
    }


    @Override
    public void deliver(String msg){

    }


    public static void main(String args[]) throws Exception {
        String ip;
        int port;
        System.out.println("Digite o IP");
        Scanner scanner = new Scanner(System.in);
        ip = scanner.nextLine();
        System.out.println("Digite a porta");
        port = scanner.nextInt();
        Client client = new Client(ip, port);
    }

}
