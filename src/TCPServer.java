import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;

public class TCPServer {
    private final int PORT = 40000;
    private final int BUFFER_SIZE = 1024;
    private final int SEQ_BYTES = 4;

    public static void main(String[] args) {
        TCPServer ss = new TCPServer();
        ss.createUDPServer(args);
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

    private boolean postImgToUDP(String filename, byte[] totalbytes, String[] iparg) {
        try {
            // First Calculate the number of packets to be sent
            int n = totalbytes.length;
            int numPackets = (int) Math.ceil(n / (double) BUFFER_SIZE);

            /* ---------- THEN SEND THE HEADER TO THE UDP SERVER ----------
            *  The header includes (in that order): filename, request, numPackets */
            String mssgStr = filename + "\r\n" + "POST\r\n" + numPackets + "\r\n";
            byte[] mssgBytes = mssgStr.getBytes();

            int UDP_PORT = 41000;
            InetAddress IPAddress = InetAddress.getByName(iparg[0]);
            DatagramSocket datagramSocket = new DatagramSocket();

            DatagramPacket packet = new DatagramPacket(mssgBytes, mssgBytes.length, IPAddress, UDP_PORT);
            datagramSocket.send(packet);
            // -------------------- THE HEADER HAS BEEN SENT -------------------- //

            byte[] sequencedBytes = createSeqArray(totalbytes, n);
            ByteArrayInputStream img_bais = new ByteArrayInputStream(sequencedBytes);

            int packetSeq;
            boolean resend_flag = false;
            byte[] img_buf = new byte[BUFFER_SIZE + 4];
            for (int i = 0; i < numPackets; i++){
                packetSeq = i + 1;
                // ------------- Send a sequenced packet to the UDP server ------------- //
                // Attempt to resend a packet that was not acknowledged
                if (resend_flag)
                    datagramSocket.send(packet);

                // Attempt to send current sequenced packet
                else {
                    int len = img_bais.read(img_buf);
                    packet = new DatagramPacket(img_buf, len, IPAddress, UDP_PORT);
                    datagramSocket.send(packet);
                }

                // check for response from the UDP server for acknowledgement of packets
                byte[] ackBytes = new byte[4];
                DatagramPacket ackPckt = new DatagramPacket(ackBytes, 4, IPAddress, UDP_PORT);
                datagramSocket.receive(ackPckt);
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

            // GET RESPONSE BYTES FROM THE UDP SERVER
            ByteArrayOutputStream res_buf = new ByteArrayOutputStream();
            byte[] receive_buf = new byte[BUFFER_SIZE];
            packet = new DatagramPacket(receive_buf, receive_buf.length, IPAddress, UDP_PORT);
            datagramSocket.receive(packet);
            res_buf.write(packet.getData());

            // Convert the first line of response into string and parse it out.
            // Since the response bytes are gauranteed to be less than 1024 bytes we only
            // need to run it once.
            BufferedReader readData = new BufferedReader(new InputStreamReader(
                    new ByteArrayInputStream(res_buf.toByteArray()), StandardCharsets.UTF_8));
            String getMessage = readData.readLine();

            readData.close();
            res_buf.close();
            datagramSocket.close();

            return getMessage.startsWith("SUCCESS");
        }
        catch (IOException ioe) {
            System.out.println(ioe.getMessage());
            return false;
        }
    }

    private boolean postImgFile(String filename, byte[] totalbytes) {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            ByteArrayInputStream bais = new ByteArrayInputStream(totalbytes);
            int j;
            while ((j = bais.read()) != -1)
                fos.write(j);

            fos.close();
            bais.close();
            return true;
        }
        catch (IOException ioe){
            System.out.println(ioe.getMessage());
            return false;
        }
    }

    private void createUDPServer(String[] iparg) {
        ServerSocket serverSocket;
        InputStream inp;
        BufferedReader br;
        ByteArrayOutputStream buffer;
        OutputStream output;
        String response;

        try {
            serverSocket = new ServerSocket(PORT);
            System.out.println("Listening on port: " + PORT);
            while (true) {
                // ACCEPT A CLIENT AND INTERPRET THEIR MESSAGE
                Socket s = serverSocket.accept();
                inp = s.getInputStream();

                // Read all contents of the inputstream into a buffer array output stream
                byte[] buf_temp = new byte[BUFFER_SIZE];
                buffer = new ByteArrayOutputStream();
                while (inp.read(buf_temp) != -1) {
                    // Write the data to the output stream
                    buffer.write(buf_temp);
                    if (inp.available() == 0){
                        break;
                    }
                }

                // Make a buffered reader to process the buffer array
                br = new BufferedReader(new InputStreamReader(
                        new ByteArrayInputStream(buffer.toByteArray()),
                        StandardCharsets.UTF_8));

                String line;
                int skipBytes = 2;
                String filename = null;
                String mode = null;
                String ctype = null;
                boolean img_flag = false;
                boolean too_big = false;
                while ((line = br.readLine()).length() != 0) {
                    System.out.println(line);
                    skipBytes += line.getBytes().length + 2;
                    if (line.startsWith("POST")){
                        mode = "POST";
                        String[] linelst = line.split(" ");
                        filename = linelst[1].substring(1);
                    }
                    else if (line.startsWith("GET")){
                        mode = "GET";
                        String[] linelst = line.split(" ");
                        filename = linelst[1].substring(1);
                    }
                    else if (line.startsWith("Content-Type")){
                        String[] linelst = line.split(" ");
                        ctype = linelst[1];
                        if (ctype.startsWith("image") && !img_flag)
                            img_flag = true;
                    }
                    else if (line.startsWith("Expect"))
                        too_big = true;
                }

                output =  s.getOutputStream();

                if (too_big) {
                    response = "HTTP/1.1 100 CONTINUE\r\n\r\n";
                    output.write(response.getBytes(StandardCharsets.UTF_8));

                    buffer.close();
                    buffer = new ByteArrayOutputStream();

                    byte[] new_buf_temp = new byte[BUFFER_SIZE];
                    while (inp.read(new_buf_temp) != -1) {
                        // Write the data to the output stream
                        buffer.write(new_buf_temp);
                        if (inp.available() == 0){
                            break;
                        }
                    }
                    skipBytes = 0;
                }

                br.close();

                // STORE DATA FROM A CLIENT AND SEND BACK THE SUCCESS STATUS
                if (mode != null && mode.equals("POST")){
                    // Skip the first bytes containing the headers to the payload
                    byte[] buf_arr = buffer.toByteArray();
                    buffer.close();
                    byte[] totalbytes = new byte[buf_arr.length - skipBytes];
                    System.arraycopy(buf_arr, skipBytes, totalbytes, 0, totalbytes.length);

                    // FILE IS AN IMAGE
                    if (img_flag) {
                        boolean file_written = postImgToUDP(filename, totalbytes, iparg);

                        if (file_written) {
                            System.out.println("File written successfully");
                            Date date = new Date();
                            response = "HTTP/1.1 201 CREATED\r\n" +
                                    "Date: " + date + "\r\n" +
                                    "Content-Type: " + ctype + "\r\n\r\n";
                            output.write(response.getBytes());
                        }
                        else {
                            response = "HTTP/1.1 404 ERROR\r\n\r\n";
                            output.write(response.getBytes());
                        }
                    }

                    // FILE IS NOT AN IMAGE
                    else {
                        // Write the bytes to the desired file
                        boolean file_written = postImgFile(filename, totalbytes);
                        if (file_written) {
                            Date date = new Date();
                            response = "HTTP/1.1 201 CREATED\r\n" +
                                    "Date: " + date + "\r\n" +
                                    "Content-Type: " + ctype + "\r\n\r\n";
                            output.write(response.getBytes());
                        }

                        else {
                            response = "HTTP/1.1 404 ERROR\r\n\r\n";
                            output.write(response.getBytes());
                        }
                    }
                }

                // SEND A RESPONSE TO THE CLIENT FOR GET REQUEST
                else if (mode != null && mode.equals("GET")){
                    if (img_flag) {
                        // FIRST SEND THE HEADERS TO THE UDP SERVER
                        String mssgStr = filename + "\r\n" + "GET\r\n";
                        int mssg_len = mssgStr.getBytes().length;

                        int UDP_PORT = 41000;
                        DatagramSocket datagramSocket = new DatagramSocket();
                        InetAddress IPAddress  = InetAddress.getByName(iparg[0]);

                        DatagramPacket packet = new DatagramPacket(mssgStr.getBytes(), mssg_len, IPAddress, UDP_PORT);
                        datagramSocket.send(packet);
                        // ---------- HEADERS HAVE BEEN SENT ---------- //

                        // RECEIVE SUCCESS/FAILURE DATA BACK FROM THE UDP SERVER
                        ByteArrayOutputStream res_buf = new ByteArrayOutputStream();
                        byte[] receive_buf = new byte[BUFFER_SIZE];
                        packet = new DatagramPacket(receive_buf, BUFFER_SIZE, IPAddress, UDP_PORT);
                        datagramSocket.receive(packet);
                        res_buf.write(packet.getData());

                        // CONVERT THE FIRST LINE TO STRING FOR PARSING TO SUCCESS/FAILURE
                        br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(res_buf.toByteArray()),
                                StandardCharsets.UTF_8));
                        String getMessage = br.readLine();

                        if (getMessage.startsWith("SUCCESS")) {
                            // Make another request to the server to get the number of packets
                            int numPackets = Integer.parseInt(br.readLine());
                            br.close();

                            HashMap<Integer, byte[]> packetMap = new HashMap<>();
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            byte[] seqBuffer = new byte[BUFFER_SIZE + SEQ_BYTES];
                            DatagramPacket seqRecievePacket = new DatagramPacket(seqBuffer,BUFFER_SIZE + SEQ_BYTES);
                            int currNumPackets = 0;
                            while (true) {
                                datagramSocket.receive(seqRecievePacket);
                                byte[] img_data = seqRecievePacket.getData();
                                int seqNum = ByteBuffer.wrap(img_data).getInt();

                                if (!packetMap.containsKey(seqNum)) {
                                    int packetSize = seqRecievePacket.getLength();
                                    byte[] imgBytes = new byte[packetSize - SEQ_BYTES];
                                    System.arraycopy(img_data, SEQ_BYTES, imgBytes, 0, packetSize - SEQ_BYTES);
                                    packetMap.put(seqNum, imgBytes);
                                    currNumPackets++;

                                    // SEND ACKNOWLEDGEMENT
                                    byte[] send_seqAck = ByteBuffer.allocate(SEQ_BYTES).putInt(seqNum).array();
                                    DatagramPacket sendPacket = new DatagramPacket(send_seqAck, SEQ_BYTES, IPAddress, UDP_PORT);
                                    datagramSocket.send(sendPacket);
                                }

                                if (currNumPackets >= numPackets) {
                                    for (int i = 0; i < numPackets; i++) {
                                        int curr_seq = i + 1;
                                        byte[] curr_packet = packetMap.get(curr_seq);
                                        bos.write(curr_packet);
                                    }

                                    byte[] bosarr = bos.toByteArray();
                                    ByteArrayInputStream succ_bais = new ByteArrayInputStream(bosarr);

                                    Date date = new Date();
                                    response = "HTTP/1.1 200 OK\r\n" +
                                    "Date: " + date + "\r\n" +
                                    "Content-Type: " + ctype + "\r\n" +
                                    "Content-Length: " + bosarr.length + "\r\n\r\n";
                                    output.write(response.getBytes());

                                    byte[] img_resbt = new byte[BUFFER_SIZE];
                                    while (succ_bais.read(img_resbt) != -1)
                                        output.write(img_resbt);
                                }
                            }
                        }

                        datagramSocket.close();
                    }

                    else {
                        File getF = new File(filename);
                        if (getF.exists()) {
                            FileInputStream fis = new FileInputStream(getF);
                            Date date = new Date();
                            response = "HTTP/1.1 200 OK\r\n" +
                                    "Date: " + date + "\r\n" +
                                    "Content-Type: " + ctype + "\r\n" +
                                    "Content-Length: " + fis.available() + "\r\n\r\n";
                            output.write(response.getBytes());

                            int bread;
                            while ((bread = fis.read()) != -1)
                                output.write(bread);

                            fis.close();
                        }
                        else {
                            response = "HTTP/1.1 404 ERROR\r\n\r\n";
                            output.write(response.getBytes());
                        }
                    }
                }

                else {
                    response = "HTTP/1.1 404 ERROR\r\n\r\n";
                    output.write(response.getBytes());
                }

                output.flush();
                output.close();
                inp.close();
            }
        }

        catch (IOException e) { System.out.println(e.getMessage()); }
    }
}
