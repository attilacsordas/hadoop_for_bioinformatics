package bioinformatics.hadoop;

/**
 * Created by attilacsordas on 28/02/2014.
 */

// using http://www.philippeadjiman.com/blog/2010/01/07/hadoop-tutorial-series-issue-3-counters-in-action/

    public class StringUtils {

        public static boolean startsWithDigit(String s){
            if( s == null || s.length() == 0 )
                return false;

            return Character.isDigit(s.charAt(0));
        }

        public static boolean startsWithLetter(String s){
            if( s == null || s.length() == 0 )
                return false;

            return Character.isLetter(s.charAt(0));
        }

    public static boolean containsDigit(String s) {

        if (s == null || s.length() == 0)
            return false;

        return s.matches(".*\\d.*");
    }


    public static boolean containsUppercaseLettersOnly(String s) {

        if (s == null || s.length() == 0)
            return false;

        return s.matches("[A-Z]+");
    }

}