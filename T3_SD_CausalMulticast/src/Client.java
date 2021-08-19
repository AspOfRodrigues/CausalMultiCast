/**
 * @author Henrique Rodrigues , Roberto Menegais
 */

import CausalMulticast.CausalMulticast;
import CausalMulticast.ICausalMulticast;

import java.util.Scanner;
/**
 * Classe que implementa as funcionalidades do cliente
 */
public class Client implements ICausalMulticast {
    private CausalMulticast cm;

    /**
     * Construtor do cliente, instancia um CausalMultiCast para registrar esse processo
     *
     * @param ip ip usado no metodo de discover do CausalMultiCast para este processo
     * @param port port usado no metodo de discover do CausalMultiCast para este processo
     * @throws Exception
     */
    public Client(String ip, int port) throws Exception {
        cm = new CausalMulticast(ip, port, this);
    }

    /**
     * Metodo que implementa a funcionalidade de enviar mensagens do cliente
     */
    public void chatApp() {
        while (true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Escreva sua mensagem: ");
            String msg = scanner.nextLine();
            cm.mcsend(msg,this);
        }
    }

    /**
     * Simplesmente printa uma mensagem recebida
     * @param msg mensagem a ser recebida por este processo
     */
    @Override
    public void deliver(String msg) {
        System.out.println("Mensagem chegou: " + msg);
    }

    /**
     * main usada para definir o ip e porta que esse processo ira utilizar ao iniciar
     * apos inserir ip e porta validos, o cliente vai ser registrado e vai ser iniciada a aplicação
     * para enviar e receber mensagens
     * @param args
     * @throws Exception
     */
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
