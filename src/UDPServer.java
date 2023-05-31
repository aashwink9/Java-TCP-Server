import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class UDPServer {
    private static final int BUFFER_SIZE = 1024;
    private static final int PORT = 41000;
    private static final int seqBytes = 4;

    public static void main(String[] args) {
        UDPServer upi = new UDPServer();
        upi.startUDPServer();
    }

    private void startUDPServer() {
        try (DatagramSocket udpSocket = new DatagramSocket(PORT)) {
            System.out.println("UDP server is listening on port " + PORT + "...");

            byte[] buffer = new byte[BUFFER_SIZE];
            DatagramPacket receivePacket = new DatagramPacket(buffer, BUFFER_SIZE);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DatagramPacket sendPacket;
            HashMap<Integer, byte[]> packetMap = new HashMap<>();

            // ------------- GET THE HEADER DATA FROM THE SERVER ------------- //
            udpSocket.receive(receivePacket);
            bos.write(receivePacket.getData());

            InetAddress callerIPAddress = receivePacket.getAddress();
            int callerPort = receivePacket.getPort();

            bos.write(receivePacket.getData());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new ByteArrayInputStream(bos.toByteArray()),
                    StandardCharsets.UTF_8));

            // File name
            String fname = br.readLine();
            // Request: POST/GET
            String rqst = br.readLine();

            bos.reset();

            byte[] seqBuffer = new byte[BUFFER_SIZE + 4];
            DatagramPacket seqRecievePacket = new DatagramPacket(seqBuffer, BUFFER_SIZE + 4);
            // ----------------- HEADER DATA HAS BEEN PROCESSED ----------------- //

            if (rqst.startsWith("POST")) {
                // Number of packets
                int numPackets = Integer.parseInt(br.readLine());

                int currNumPackets = 0;
                // ---------- NOW CONTINUOUSLY LISTEN FOR UPCOMING PACKETS ---------- //
                while (true){
                    udpSocket.receive(seqRecievePacket);
                    byte[] img_data = seqRecievePacket.getData();
                    int seqNum = ByteBuffer.wrap(img_data).getInt();

                    if (!packetMap.containsKey(seqNum)) {
                        int packetSize = seqRecievePacket.getLength();
                        byte[] imgBytes = new byte[packetSize - seqBytes];
                        System.arraycopy(img_data, seqBytes, imgBytes, 0, packetSize - seqBytes);
                        packetMap.put(seqNum, imgBytes);
                        currNumPackets++;

                        // SEND ACKNOWLEDGEMENT
                        byte[] send_seqAck = ByteBuffer.allocate(seqBytes).putInt(seqNum).array();
                        sendPacket = new DatagramPacket(send_seqAck, seqBytes, callerIPAddress, callerPort);
                        udpSocket.send(sendPacket);
                    }

                    if (currNumPackets >= numPackets) {
                        for (int i = 0; i < numPackets; i++) {
                            int curr_seq = i + 1;
                            byte[] curr_packet = packetMap.get(curr_seq);
                            bos.write(curr_packet);
                        }

                        byte[] bosarr = bos.toByteArray();

                        try {
                            FileOutputStream fos = new FileOutputStream(fname);
                            ByteArrayInputStream bais = new ByteArrayInputStream(bosarr);

                            byte[] img_writeBuf = new byte[BUFFER_SIZE];
                            while (bais.read(img_writeBuf) != -1)
                                fos.write(img_writeBuf);

                            fos.close();
                            bais.close();

                            byte[] sendRes = "SUCCESS\r\n".getBytes();
                            sendPacket = new DatagramPacket(sendRes, sendRes.length,
                                    callerIPAddress, callerPort);
                            udpSocket.send(sendPacket);

                        } catch (IOException ioe) {
                            System.err.println(ioe.getMessage());
                            byte[] sendRes = "FAILURE\r\n".getBytes();
                            sendPacket = new DatagramPacket(sendRes, sendRes.length,
                                    callerIPAddress, callerPort);
                            udpSocket.send(sendPacket);
                        }
                    }
                }
            }

            else if (rqst.startsWith("GET")) {
                File getF = new File(fname);
                if (getF.exists()) {
                    try (FileInputStream fis = new FileInputStream(getF)) {

                        byte[] img_buf = new byte[BUFFER_SIZE];
                        while (fis.read(img_buf) != -1)
                            bos.write(img_buf);

                        byte[] imgBytes = bos.toByteArray();
                        int n = imgBytes.length;
                        int numPackets = (int) Math.ceil(n / (double) BUFFER_SIZE);

                        String message = "SUCCESS\r\n" + numPackets + "\r\n";
                        byte[] sendRes = message.getBytes();
                        sendPacket = new DatagramPacket(sendRes, sendRes.length, callerIPAddress, callerPort);
                        udpSocket.send(sendPacket);

                        byte[] seqImgBytes = createSeqArray(imgBytes, n);
                        ByteArrayInputStream img_bais = new ByteArrayInputStream(seqImgBytes);

                        int packetSeq;
                        boolean resend_flag = false;
                        byte[] imgSeqBuf = new byte[BUFFER_SIZE + 4];
                        for (int i = 0; i < numPackets; i++){
                            packetSeq = i + 1;
                            // ------------- Send a sequenced packet to the UDP server ------------- //
                            // Attempt to resend a packet that was not acknowledged
                            if (resend_flag)
                                udpSocket.send(sendPacket);

                            // Attempt to send current sequenced packet
                            else {
                                int len = img_bais.read(imgSeqBuf);
                                sendPacket = new DatagramPacket(imgSeqBuf, len, callerIPAddress, callerPort);
                                udpSocket.send(sendPacket);
                            }

                            // check for response from the UDP server for acknowledgement of packets
                            byte[] ackBytes = new byte[4];
                            DatagramPacket ackPckt = new DatagramPacket(ackBytes, 4, callerIPAddress, callerPort);
                            udpSocket.receive(ackPckt);

                            int packetSeqAck = ByteBuffer.wrap(ackPckt.getData()).getInt();
                            if (packetSeqAck == packetSeq && resend_flag)
                                resend_flag = false;

                            else if (packetSeqAck != packetSeq && !resend_flag) {
                                resend_flag = true;
                                i--;
                            }

                            else if (packetSeqAck != packetSeq)
                                i--;
                        }

                    } catch (IOException ioe) {
                        System.err.println(ioe.getMessage());
                        byte[] sendRes = "FAILURE\r\n".getBytes();
                        sendPacket = new DatagramPacket(sendRes, sendRes.length,
                                callerIPAddress, callerPort);
                        udpSocket.send(sendPacket);
                    }
                } else {
                    byte[] sendRes = "FAILURE\r\n".getBytes();
                    sendPacket = new DatagramPacket(sendRes, sendRes.length,
                            callerIPAddress, callerPort);
                    udpSocket.send(sendPacket);
                }
            }
        } catch (IOException e) {
            System.err.println("Socket error: " + e.getMessage());
        }
    }

    private byte[] createSeqArray(byte[] totalbytes, int n) throws IOException {
        int seqNumSize = 4;
        int chunks = n / BUFFER_SIZE;
        int last_byts = n % BUFFER_SIZE;
        if (last_byts != 0)
            chunks++;

        ByteArrayInputStream bhg = new ByteArrayInputStream(totalbytes);
        ByteArrayOutputStream bsd = new ByteArrayOutputStream();

        byte[] chunkBuf = new byte[BUFFER_SIZE];
        for (int i = 0; i < chunks; i++) {
            int seqNum = i + 1;
            byte[] seqNumBytes = ByteBuffer.allocate(seqNumSize).putInt(seqNum).array();
            bsd.write(seqNumBytes);

            int len = bhg.read(chunkBuf);

            if (len < BUFFER_SIZE){
                byte[] smaller_buf = new byte[len];
                System.arraycopy(chunkBuf, 0, smaller_buf, 0, len);
                bsd.write(smaller_buf);
            }
            else {
                bsd.write(chunkBuf);
            }
        }
        return bsd.toByteArray();
    }
}
