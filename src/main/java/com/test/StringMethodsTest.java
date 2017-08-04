/**
 * @author Jeck
 *
 * Create on 2017年7月21日 下午5:20:55
 */
package com.test;

/**
 * @author Jeck
 *
 * Create on 2017年7月21日 下午5:20:55
 */
public class StringMethodsTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// String st = "bananatest中文";
	// System.out.println("st.indexOf(\"b\")=" + st.indexOf("b"));
	// System.out.println("st.indexOf(\"na\")=" + st.indexOf("na"));
	// System.out.println("st.indexOf(\"na\")=" + st.indexOf("na", 3));
	// System.out.println("st.indexOf(\"test\")=" + st.indexOf("test"));
	//
	// System.out.println("st.charAt(0)=" + st.charAt(0));
	// System.out.println("st.charAt(0)=" + st.charAt(6));
	//
	// System.out.println("st.codePointAt(0)=" + st.codePointAt(0));
	// System.out.println("st.codePointAt(6)=" + st.codePointAt(6));
	//
	// System.out.println("st.length()=" + st.length());

	Long st = 0L;
	Long ed = Long.parseLong("FFFF", 16);
	// Long ed = 655350L;

	// System.out.println(String.format("st=%1$04x, end=%2$04x", st, ed));
	//
	// System.out.println(String.format("st=%1$04d, end=%2$04d", st, ed));
	//
	// System.out.println(String.format("655350=%1$04x", 655350));
	// System.out.println(655350 % 16);
	// System.out.println(655350 / 16 % 16);
	// System.out.println(655350 / 16 / 16 / 16 / 16 % 16);


	for(int i=0; i<100; i++){
	    System.out.println(String.format("st=%1$04d", i));
	}

    }

}
