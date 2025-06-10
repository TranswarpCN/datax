package com.alibaba.datax.plugin.reader.argodbreader;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class IpUtils {
  /**
   * 获取Linux下的IP地址
   *
   * @return IP地址
   * @throws SocketException
   */
  public static String getLinuxLocalIp() throws SocketException {
    String ip = "";
    try {
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
        NetworkInterface intf = en.nextElement();
        String name = intf.getName();
        if (!name.contains("docker") && !name.contains("lo")) {
          for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();) {
            InetAddress inetAddress = enumIpAddr.nextElement();
            if (!inetAddress.isLoopbackAddress()) {
              String ipaddress = inetAddress.getHostAddress().toString();
              if (!ipaddress.contains("::") && !ipaddress.contains("0:0:") && !ipaddress.contains("fe80")) {
                ip = ipaddress;
                System.out.println(ipaddress);
              }
            }
          }
        }
      }
    } catch (SocketException ex) {
      System.out.println("获取ip地址异常");
      ip = "127.0.0.1";
      ex.printStackTrace();
    }
    System.out.println("IP:"+ip);
    return ip;
  }

  public static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  public static void main(String[] args) throws SocketException, UnknownHostException {
    System.out.println(getLinuxLocalIp());
    System.out.println(getLocalHostName());
  }
}
