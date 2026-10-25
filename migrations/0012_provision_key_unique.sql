DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM device_profile
        WHERE provision_type IN ('ALLOW_CREATE_NEW_DEVICES', 'CHECK_PRE_PROVISIONED_DEVICES')
          AND profile_data->'provisionConfiguration'->>'provisionDeviceKey' IS NOT NULL
        GROUP BY profile_data->'provisionConfiguration'->>'provisionDeviceKey'
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate provisionDeviceKey values must be resolved before enabling the unique constraint';
    END IF;
END
$$;

CREATE UNIQUE INDEX idx_device_profile_provision_key_unique
    ON device_profile ((profile_data->'provisionConfiguration'->>'provisionDeviceKey'))
    WHERE provision_type IN ('ALLOW_CREATE_NEW_DEVICES', 'CHECK_PRE_PROVISIONED_DEVICES')
      AND profile_data->'provisionConfiguration'->>'provisionDeviceKey' IS NOT NULL;
