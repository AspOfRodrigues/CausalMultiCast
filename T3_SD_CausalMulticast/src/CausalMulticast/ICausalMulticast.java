package CausalMulticast;


public interface ICausalMulticast {
    /**
     * Método usado como callback pelo CausalMulticast para entrega da mensagem ao cliente. Todos clientes que desejam usar o CausalMulticast devem implementar essa interface e esse método.
     * @param msg mensagem a ser recebida por este cliente
     */
    public void deliver(String msg);
}
