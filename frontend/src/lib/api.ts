// Transitional mixed-family compatibility barrel.
// New cUSE4-owned frontend code should import from `@/lib/cuse4Api`.
// New cPAR-owned frontend code should import from `@/lib/cparApi`.

import { ApiError, apiFetch } from "@/lib/apiTransport";
import { cparApiPath } from "@/lib/cparApi";
import { cuse4ApiPath } from "@/lib/cuse4Api";

export { ApiError, apiFetch };

export const apiPath = {
  ...cuse4ApiPath,
  ...cparApiPath,
} as const;
