package org.thingsboard;

import org.eclipse.leshan.client.LeshanClient;
import org.eclipse.leshan.client.LeshanClientBuilder;
import org.eclipse.leshan.client.californium.endpoint.CaliforniumClientEndpointsProvider;
import org.eclipse.leshan.client.object.Device;
import org.eclipse.leshan.client.object.Security;
import org.eclipse.leshan.client.object.Server;
import org.eclipse.leshan.client.resource.BaseInstanceEnabler;
import org.eclipse.leshan.client.resource.ObjectsInitializer;
import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.model.ObjectLoader;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.StaticModel;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.request.BindingMode;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class LeshanInteropClient {
    public static final class Temperature extends BaseInstanceEnabler {
        private volatile float value = 42.0f;

        @Override
        public ReadResponse read(LwM2mServer identity, int resourceId) {
            if (resourceId == 5700) {
                return ReadResponse.success(resourceId, value);
            }
            return ReadResponse.notFound();
        }

        void notifyValue(float next) {
            value = next;
            fireResourceChange(5700);
        }
    }

    public static void main(String[] args) throws Exception {
        String serverUri = args[0];
        String endpoint = args[1];
        Temperature temperature = new Temperature();
        List<ObjectModel> models = new ArrayList<>(ObjectLoader.loadAllDefault());
        models.addAll(ObjectLoader.loadDdfFile(
            LeshanInteropClient.class.getResourceAsStream("/3303.xml"), "3303.xml"));
        ObjectsInitializer initializer = new ObjectsInitializer(new StaticModel(models));
        initializer.setInstancesForObject(LwM2mId.SECURITY, Security.noSec(serverUri, 12345));
        initializer.setInstancesForObject(LwM2mId.SERVER, new Server(12345, 60));
        initializer.setInstancesForObject(LwM2mId.DEVICE,
            new Device("Eclipse Leshan", "model12345", "12345", EnumSet.of(BindingMode.U)));
        initializer.setInstancesForObject(3303, temperature);
        LeshanClientBuilder builder = new LeshanClientBuilder(endpoint);
        builder.setEndpointsProviders(new CaliforniumClientEndpointsProvider());
        builder.setObjects(initializer.createAll());
        LeshanClient client = builder.build();
        client.start();
        try {
            Thread.sleep(5000);
            temperature.notifyValue(77.0f);
            Thread.sleep(3000);
        } finally {
            client.destroy(true);
        }
    }
}
