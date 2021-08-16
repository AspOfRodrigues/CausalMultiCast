import CausalMultiCast.CausalMultiCast;
import java.util.Scanner;

public class Client {


    public Client(String ip, int port) throws Exception {
        new CausalMultiCast(ip,port);
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
