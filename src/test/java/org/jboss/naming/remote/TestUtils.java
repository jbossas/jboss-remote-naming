package org.jboss.naming.remote;

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.jboss.remoting3.security.AuthorizingCallbackHandler;
import org.jboss.remoting3.security.ServerAuthenticationProvider;
import org.jboss.remoting3.security.SimpleUserInfo;
import org.jboss.remoting3.security.UserInfo;
import org.xnio.OptionMap;
import org.xnio.Property;
import org.xnio.Sequence;

import static org.xnio.Options.SASL_MECHANISMS;
import static org.xnio.Options.SASL_POLICY_NOANONYMOUS;
import static org.xnio.Options.SASL_PROPERTIES;
import static org.xnio.Options.SSL_ENABLED;

/**
 * @author Stuart Douglas
 */
public class TestUtils {

    public static final String ANONYMOUS = "ANONYMOUS";

    public static OptionMap createOptionMap() {
        OptionMap.Builder builder = OptionMap.builder();
        builder.set(SSL_ENABLED, false);
        builder.set(SASL_MECHANISMS, Sequence.<String>of(ANONYMOUS));
        builder.set(SASL_PROPERTIES, Sequence.<Property>empty());
        builder.set(SASL_POLICY_NOANONYMOUS, false);

        return builder.getMap();
    }

    public static class DefaultAuthenticationHandler implements ServerAuthenticationProvider {
        @Override
        public AuthorizingCallbackHandler getCallbackHandler(String mechanismName) {
            if (mechanismName.equals(ANONYMOUS)) {
                return new AuthorizingCallbackHandler() {
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback current : callbacks) {
                            throw new UnsupportedCallbackException(current, "ANONYMOUS mechanism so not expecting a callback");
                        }
                    }

                    @Override
                    public UserInfo createUserInfo(Collection<Principal> remotingPrincipals) throws IOException {
                        if (remotingPrincipals == null) {
                            return null;
                        }
                        return new SimpleUserInfo(remotingPrincipals);
                    }
                };
            }
            return null;
        }
    }

    public static class AnonymousCallbackHandler implements CallbackHandler {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback current : callbacks) {
                if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName(ANONYMOUS);
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }
    }

}
