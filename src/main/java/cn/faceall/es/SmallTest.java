package cn.faceall.es;

import org.joda.time.DateTime;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;

public class SmallTest {

    public static void main(String[] args){

//        DecimalFormat decimalFormat = new DecimalFormat("0.0000");
//        double number = 10.123456789012345;
//        double number_short = Double.parseDouble(decimalFormat.format(number));
//        System.out.println((int)number_short);
//        System.out.println(number_short*number_short);

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar tmp = Calendar.getInstance();
        tmp.setTime(new Date());
        tmp.set(Calendar.YEAR, 2017);
        tmp.set(Calendar.MONTH, 2);
        tmp.set(Calendar.DAY_OF_MONTH, 28);
        tmp.set(Calendar.HOUR_OF_DAY, 5);
        tmp.set(Calendar.MINUTE, 24);
        tmp.set(Calendar.SECOND, 6);
        System.out.println(tmp.getTime());
        System.out.println(df.format(tmp.getTime()));

//        Date date = new Date();
//        date.setTime(Calendar.getInstance(new SimpleTimeZone(0, "GMT")).getTimeInMillis());
//        System.out.println(date.toString());

//        System.out.println("joda-time：现在的时间是："+new DateTime().toString().split("T")[0]);

    }

}
