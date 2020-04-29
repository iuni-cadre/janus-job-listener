package iu.cadre.listeners.job;

import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.process.traversal.P;

public class RegexPredicate implements BiPredicate<Object, Object> {
    Pattern pattern = null;
    private Mode mode;

    enum Mode {
        FIND, MATCH
    }

    public RegexPredicate(String regex, Mode mode) {
        this.mode = mode;
        pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    };

    public RegexPredicate(String regex) {
        this(regex,Mode.FIND);
    }

    @Override
    public boolean test(final Object first, final Object second) {
        String str = first.toString();
        Matcher matcher = pattern.matcher(str);
        switch (mode) {
            case FIND:
                return matcher.find();
            case MATCH:
                return matcher.matches();
        }
        return false;
    }

    /**
     * get a Regular expression predicate
     *
     * @param regex
     * @return - the predicate
     */
    public static P<Object> regex(Object regex) {
        BiPredicate<Object, Object> b = new RegexPredicate(regex.toString());
        return new P<Object>(b, regex);
    }
}
