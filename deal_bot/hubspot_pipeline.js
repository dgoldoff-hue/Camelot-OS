/**
 * hubspot_pipeline.js — Camelot OS Deal Bot
 * ==========================================
 * Node.js HubSpot CRM integration for the "Camelot Roll-Up" acquisition pipeline.
 *
 * Pipeline: "Camelot Roll-Up"
 * Stages: Identified → Contacted → Responded → Meeting Scheduled → Term Sheet → Closed
 *
 * Exports:
 *   - upsertProspect(companyData)        Upsert a deal + company + contact
 *   - logOutreach(dealId, channel, msg)  Log outreach activity on deal timeline
 *   - updateDealStage(dealId, stage)     Move deal to new pipeline stage
 *   - getDeal(dealId)                    Fetch deal details
 *   - getDealsInStage(stageId)           List deals in a pipeline stage
 *   - searchDeals(query)                 Search deals by company name
 *
 * Uses only Node.js built-in `https` module — no axios.
 *
 * Author: Camelot OS
 */

'use strict';

const https = require('https');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const HUBSPOT_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN;
if (!HUBSPOT_TOKEN) {
  throw new Error('HUBSPOT_ACCESS_TOKEN environment variable is required');
}

const BASE_URL = 'api.hubapi.com';

// Pipeline and stage identifiers — resolved at startup from HubSpot API
// These are the label names; we resolve to IDs dynamically to avoid hardcoding.
const PIPELINE_NAME = 'Camelot Roll-Up';
const STAGE_LABELS = [
  'Identified',
  'Contacted',
  'Responded',
  'Meeting Scheduled',
  'Term Sheet',
  'Closed',
];

// Stage label → normalized key (for use in API calls)
const STAGE_KEY_MAP = {
  'identified':        'Identified',
  'contacted':         'Contacted',
  'responded':         'Responded',
  'meeting_scheduled': 'Meeting Scheduled',
  'meeting-scheduled': 'Meeting Scheduled',
  'term_sheet':        'Term Sheet',
  'term-sheet':        'Term Sheet',
  'closed':            'Closed',
  'closedwon':         'Closed',
};

// Cache pipeline info after first fetch
let _pipelineCache = null;

// ---------------------------------------------------------------------------
// HTTP helper (no axios — pure https module)
// ---------------------------------------------------------------------------

/**
 * Make an authenticated HubSpot API request.
 *
 * @param {string} method   HTTP method (GET, POST, PATCH, DELETE)
 * @param {string} path     API path (e.g. '/crm/v3/objects/deals')
 * @param {object|null} body Request body (will be JSON-serialized)
 * @returns {Promise<{statusCode: number, data: any}>}
 */
function hubspotRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;

    const options = {
      hostname: BASE_URL,
      port: 443,
      path,
      method,
      headers: {
        Authorization: `Bearer ${HUBSPOT_TOKEN}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
    };

    if (bodyStr) {
      options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
    }

    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        let data;
        try {
          data = raw ? JSON.parse(raw) : {};
        } catch {
          data = { raw };
        }

        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ statusCode: res.statusCode, data });
        } else {
          const err = new Error(
            `HubSpot API error ${res.statusCode} on ${method} ${path}: ` +
            (data.message || JSON.stringify(data))
          );
          err.statusCode = res.statusCode;
          err.responseData = data;
          reject(err);
        }
      });
    });

    req.on('error', (err) => {
      reject(new Error(`Network error calling HubSpot: ${err.message}`));
    });

    if (bodyStr) {
      req.write(bodyStr);
    }
    req.end();
  });
}

/**
 * Retry wrapper — retries on 429 (rate limit) or 5xx errors.
 *
 * @param {Function} fn       Async function returning a promise
 * @param {number}   retries  Max retry attempts (default 4)
 * @returns {Promise<any>}
 */
async function withRetry(fn, retries = 4) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const retryable = err.statusCode === 429 || (err.statusCode >= 500);
      if (!retryable || attempt === retries) throw err;

      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
      console.warn(
        `[hubspot_pipeline] Retry ${attempt}/${retries} after ${delay}ms — ${err.message}`
      );
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

// ---------------------------------------------------------------------------
// Pipeline / Stage resolver
// ---------------------------------------------------------------------------

/**
 * Fetch and cache the Camelot Roll-Up pipeline details.
 * Returns { pipelineId, stages: { 'Identified': 'stageId', ... } }
 */
async function getPipelineInfo() {
  if (_pipelineCache) return _pipelineCache;

  const { data } = await withRetry(() =>
    hubspotRequest('GET', '/crm/v3/pipelines/deals')
  );

  const pipeline = (data.results || []).find(
    (p) => p.label === PIPELINE_NAME
  );

  if (!pipeline) {
    throw new Error(
      `Pipeline "${PIPELINE_NAME}" not found in HubSpot. ` +
      `Available: ${(data.results || []).map((p) => p.label).join(', ')}`
    );
  }

  const stages = {};
  for (const stage of pipeline.stages || []) {
    stages[stage.label] = stage.id;
  }

  _pipelineCache = { pipelineId: pipeline.id, stages };
  console.log(`[hubspot_pipeline] Loaded pipeline "${PIPELINE_NAME}" (${pipeline.id})`);
  return _pipelineCache;
}

/**
 * Resolve a stage label/key to its HubSpot stage ID.
 *
 * @param {string} stageInput  Stage label or normalized key
 * @returns {Promise<string>} HubSpot stage ID
 */
async function resolveStageId(stageInput) {
  const { stages } = await getPipelineInfo();
  // Try direct label lookup
  if (stages[stageInput]) return stages[stageInput];
  // Try normalized key map
  const label = STAGE_KEY_MAP[stageInput.toLowerCase()];
  if (label && stages[label]) return stages[label];
  throw new Error(
    `Unknown stage "${stageInput}". Valid stages: ${Object.keys(stages).join(', ')}`
  );
}

// ---------------------------------------------------------------------------
// Company operations
// ---------------------------------------------------------------------------

/**
 * Find an existing HubSpot company by name or domain.
 *
 * @param {string} name    Company name
 * @param {string} domain  Company website domain (optional)
 * @returns {Promise<string|null>} HubSpot company ID or null
 */
async function findCompany(name, domain = '') {
  const filters = [];
  if (name) {
    filters.push({ propertyName: 'name', operator: 'CONTAINS_TOKEN', value: name.split(' ')[0] });
  }

  const payload = {
    filterGroups: [{ filters }],
    properties: ['name', 'domain'],
    limit: 10,
  };

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/crm/v3/objects/companies/search', payload)
  );

  const results = data.results || [];
  // Prefer exact name match
  const exact = results.find(
    (c) => (c.properties.name || '').toLowerCase() === name.toLowerCase()
  );
  if (exact) return exact.id;

  // Domain match
  if (domain) {
    const domainMatch = results.find(
      (c) => (c.properties.domain || '').toLowerCase().includes(domain.toLowerCase())
    );
    if (domainMatch) return domainMatch.id;
  }

  return results.length > 0 ? results[0].id : null;
}

/**
 * Create or update a HubSpot company for a prospect.
 *
 * @param {object} companyData
 * @returns {Promise<string>} HubSpot company ID
 */
async function upsertCompany(companyData) {
  const existingId = await findCompany(companyData.name, companyData.domain || '');

  const properties = {
    name: companyData.name,
    phone: companyData.phone || '',
    website: companyData.website || '',
    address: companyData.address || '',
    city: companyData.city || '',
    state: companyData.state || '',
    zip: companyData.zipCode || '',
    description: companyData.description || '',
    numberofemployees: String(companyData.estimatedUnits || ''),
    // Custom Camelot properties
    camelot_fit_score: String(companyData.fitScore || ''),
    camelot_recommended_angle: companyData.recommendedAngle || '',
    camelot_recommended_structure: companyData.recommendedStructure || '',
    camelot_estimated_units: String(companyData.estimatedUnits || ''),
    camelot_pain_points: (companyData.painPoints || []).join('; '),
    camelot_geographies: (companyData.geographies || []).join(', '),
  };

  if (existingId) {
    await withRetry(() =>
      hubspotRequest('PATCH', `/crm/v3/objects/companies/${existingId}`, { properties })
    );
    console.log(`[hubspot_pipeline] Updated company ${existingId}: ${companyData.name}`);
    return existingId;
  }

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/crm/v3/objects/companies', { properties })
  );
  console.log(`[hubspot_pipeline] Created company ${data.id}: ${companyData.name}`);
  return data.id;
}

// ---------------------------------------------------------------------------
// Contact operations
// ---------------------------------------------------------------------------

/**
 * Find an existing contact by email.
 *
 * @param {string} email
 * @returns {Promise<string|null>} Contact ID or null
 */
async function findContact(email) {
  if (!email) return null;
  try {
    const { data } = await withRetry(() =>
      hubspotRequest('GET', `/crm/v3/objects/contacts/${encodeURIComponent(email)}?idProperty=email&properties=email,firstname,lastname`)
    );
    return data.id || null;
  } catch (err) {
    if (err.statusCode === 404) return null;
    throw err;
  }
}

/**
 * Create or update a contact.
 *
 * @param {object} contactData  {email, firstName, lastName, phone, title, company}
 * @returns {Promise<string>} Contact ID
 */
async function upsertContact(contactData) {
  const existingId = await findContact(contactData.email);

  const properties = {
    email: contactData.email || '',
    firstname: contactData.firstName || '',
    lastname: contactData.lastName || '',
    phone: contactData.phone || '',
    jobtitle: contactData.title || '',
    company: contactData.company || '',
    hs_lead_status: 'IN_PROGRESS',
    lifecyclestage: 'lead',
  };

  if (existingId) {
    await withRetry(() =>
      hubspotRequest('PATCH', `/crm/v3/objects/contacts/${existingId}`, { properties })
    );
    return existingId;
  }

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/crm/v3/objects/contacts', { properties })
  );
  console.log(`[hubspot_pipeline] Created contact ${data.id}: ${contactData.email}`);
  return data.id;
}

// ---------------------------------------------------------------------------
// Deal operations
// ---------------------------------------------------------------------------

/**
 * Search for an existing deal by company name in the Roll-Up pipeline.
 *
 * @param {string} companyName
 * @returns {Promise<string|null>} Deal ID or null
 */
async function findExistingDeal(companyName) {
  const { pipelineId } = await getPipelineInfo();

  const payload = {
    filterGroups: [
      {
        filters: [
          { propertyName: 'pipeline', operator: 'EQ', value: pipelineId },
          { propertyName: 'dealname', operator: 'CONTAINS_TOKEN', value: companyName.split(' ')[0] },
        ],
      },
    ],
    properties: ['dealname', 'dealstage', 'pipeline'],
    limit: 10,
  };

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/crm/v3/objects/deals/search', payload)
  );

  const results = data.results || [];
  const match = results.find(
    (d) => (d.properties.dealname || '').toLowerCase().includes(companyName.toLowerCase().split(' ')[0])
  );
  return match ? match.id : null;
}

/**
 * Create or update (upsert) a deal in the Camelot Roll-Up pipeline.
 *
 * @param {object} companyData  Prospect data with company info, score, contacts
 * @returns {Promise<string>} HubSpot deal ID
 */
async function upsertProspect(companyData) {
  console.log(`[hubspot_pipeline] Upserting prospect: ${companyData.name || companyData.company_name}`);

  const name = companyData.name || companyData.company_name || 'Unknown Company';
  const { pipelineId, stages } = await getPipelineInfo();
  const identifiedStageId = stages['Identified'];

  if (!identifiedStageId) {
    throw new Error(`Stage "Identified" not found in pipeline "${PIPELINE_NAME}"`);
  }

  // 1. Upsert company
  const companyId = await upsertCompany({
    name,
    phone: companyData.phone || '',
    website: companyData.website || '',
    address: companyData.address || '',
    city: companyData.city || '',
    state: companyData.state || '',
    zipCode: companyData.zip_code || companyData.zipCode || '',
    estimatedUnits: companyData.estimated_units || companyData.estimatedUnits || 0,
    fitScore: companyData.fit_score || companyData.fitScore || 0,
    recommendedAngle: companyData.recommended_angle || companyData.recommendedAngle || '',
    recommendedStructure: companyData.recommended_structure || companyData.recommendedStructure || '',
    painPoints: companyData.pain_points || companyData.painPoints || [],
    geographies: companyData.geographies_served || companyData.geographies || [],
  });

  // 2. Upsert primary contact (if available)
  let contactId = null;
  const contacts = companyData.contacts || [];
  if (contacts.length > 0) {
    const c = contacts[0];
    const nameParts = (c.name || '').split(' ');
    contactId = await upsertContact({
      email: c.email || '',
      firstName: nameParts[0] || '',
      lastName: nameParts.slice(1).join(' ') || '',
      phone: c.phone || '',
      title: c.title || '',
      company: name,
    });
  }

  // 3. Check for existing deal
  const existingDealId = await findExistingDeal(name);

  const dealProperties = {
    dealname: `${name} — Roll-Up`,
    pipeline: pipelineId,
    dealstage: identifiedStageId,
    amount: String(
      (companyData.estimated_units || companyData.estimatedUnits || 0) * 50000
    ), // rough estimate: $50k per unit
    closedate: new Date(Date.now() + 180 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0],
    camelot_prospect_score: String(companyData.fit_score || companyData.fitScore || ''),
    camelot_outreach_angle: companyData.recommended_angle || companyData.recommendedAngle || '',
    camelot_deal_structure: companyData.recommended_structure || companyData.recommendedStructure || '',
    camelot_estimated_units: String(companyData.estimated_units || companyData.estimatedUnits || 0),
    description:
      `Prospect identified by Camelot Deal Bot.\n` +
      `Estimated units: ${companyData.estimated_units || 0}\n` +
      `Fit score: ${companyData.fit_score || 0}\n` +
      `Pain points: ${(companyData.pain_points || []).join('; ')}\n` +
      `Researched: ${companyData.researched_at || new Date().toISOString()}`,
  };

  let dealId;

  if (existingDealId) {
    // Update existing deal (preserve stage — don't reset to Identified)
    const { dealstage: _, ...updateProps } = dealProperties;
    await withRetry(() =>
      hubspotRequest('PATCH', `/crm/v3/objects/deals/${existingDealId}`, {
        properties: updateProps,
      })
    );
    dealId = existingDealId;
    console.log(`[hubspot_pipeline] Updated deal ${dealId}: ${name}`);
  } else {
    const { data } = await withRetry(() =>
      hubspotRequest('POST', '/crm/v3/objects/deals', { properties: dealProperties })
    );
    dealId = data.id;
    console.log(`[hubspot_pipeline] Created deal ${dealId}: ${name}`);
  }

  // 4. Associate deal with company
  if (companyId) {
    await withRetry(() =>
      hubspotRequest(
        'PUT',
        `/crm/v3/associations/deals/companies/batch/create`,
        {
          inputs: [
            {
              from: { id: dealId },
              to: { id: companyId },
              type: 'deal_to_company',
            },
          ],
        }
      )
    ).catch((err) => console.warn('[hubspot_pipeline] Company association failed:', err.message));
  }

  // 5. Associate deal with contact
  if (contactId) {
    await withRetry(() =>
      hubspotRequest(
        'PUT',
        `/crm/v3/associations/deals/contacts/batch/create`,
        {
          inputs: [
            {
              from: { id: dealId },
              to: { id: contactId },
              type: 'deal_to_contact',
            },
          ],
        }
      )
    ).catch((err) => console.warn('[hubspot_pipeline] Contact association failed:', err.message));
  }

  return dealId;
}

// ---------------------------------------------------------------------------
// Outreach logging
// ---------------------------------------------------------------------------

/**
 * Log an outreach activity on a deal's HubSpot timeline.
 *
 * @param {string} dealId   HubSpot deal ID
 * @param {string} channel  'email' | 'phone' | 'linkedin' | 'note'
 * @param {string} message  Message body or activity description
 * @param {string} subject  Subject line (for email channel)
 * @returns {Promise<string>} Created engagement ID
 */
async function logOutreach(dealId, channel, message, subject = '') {
  if (!dealId) throw new Error('dealId is required for logOutreach');

  const channelTypeMap = {
    email:    'EMAIL',
    phone:    'CALL',
    linkedin: 'NOTE',
    note:     'NOTE',
  };

  const engagementType = channelTypeMap[channel.toLowerCase()] || 'NOTE';
  const now = Date.now();

  let metadata;
  if (engagementType === 'EMAIL') {
    metadata = {
      from: {
        email: process.env.DEAL_BOT_SENDER_EMAIL || 'dgoldoff@camelot.nyc',
        firstName: 'David',
        lastName: 'Goldoff',
      },
      subject: subject || 'Camelot Outreach',
      text: message,
      status: 'SENT',
    };
  } else if (engagementType === 'CALL') {
    metadata = {
      body: message,
      status: 'COMPLETED',
      durationMilliseconds: 0,
    };
  } else {
    metadata = { body: message };
  }

  const payload = {
    engagement: {
      active: true,
      type: engagementType,
      timestamp: now,
    },
    associations: {
      dealIds: [parseInt(dealId, 10)],
    },
    metadata,
  };

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/engagements/v1/engagements', payload)
  );

  const engagementId = data.engagement?.id || data.id;
  console.log(
    `[hubspot_pipeline] Logged ${channel} outreach on deal ${dealId} → engagement ${engagementId}`
  );

  // Move deal to "Contacted" stage if currently "Identified"
  await _advanceStageIfIdentified(dealId);

  return String(engagementId);
}

/**
 * Advance a deal from Identified → Contacted if it's still in Identified.
 */
async function _advanceStageIfIdentified(dealId) {
  try {
    const { data } = await withRetry(() =>
      hubspotRequest('GET', `/crm/v3/objects/deals/${dealId}?properties=dealstage`)
    );
    const currentStage = data.properties?.dealstage;
    const { stages } = await getPipelineInfo();

    if (currentStage === stages['Identified']) {
      await updateDealStage(dealId, 'Contacted');
    }
  } catch (err) {
    console.warn('[hubspot_pipeline] Could not auto-advance stage:', err.message);
  }
}

// ---------------------------------------------------------------------------
// Stage management
// ---------------------------------------------------------------------------

/**
 * Update the pipeline stage of a deal.
 *
 * @param {string} dealId  HubSpot deal ID
 * @param {string} stage   Stage label or key
 * @returns {Promise<void>}
 */
async function updateDealStage(dealId, stage) {
  const stageId = await resolveStageId(stage);
  await withRetry(() =>
    hubspotRequest('PATCH', `/crm/v3/objects/deals/${dealId}`, {
      properties: { dealstage: stageId },
    })
  );
  console.log(`[hubspot_pipeline] Deal ${dealId} → stage "${stage}" (${stageId})`);
}

// ---------------------------------------------------------------------------
// Retrieval helpers
// ---------------------------------------------------------------------------

/**
 * Fetch full deal details.
 *
 * @param {string} dealId
 * @returns {Promise<object>} Deal object with properties
 */
async function getDeal(dealId) {
  const { data } = await withRetry(() =>
    hubspotRequest(
      'GET',
      `/crm/v3/objects/deals/${dealId}?properties=dealname,dealstage,pipeline,amount,` +
      `closedate,camelot_prospect_score,camelot_outreach_angle,camelot_deal_structure,` +
      `camelot_estimated_units,description,createdate,hs_lastmodifieddate`
    )
  );
  return data;
}

/**
 * List all deals in a specific pipeline stage.
 *
 * @param {string} stage  Stage label or key
 * @returns {Promise<Array>} Array of deal objects
 */
async function getDealsInStage(stage) {
  const stageId = await resolveStageId(stage);
  const { pipelineId } = await getPipelineInfo();

  const payload = {
    filterGroups: [
      {
        filters: [
          { propertyName: 'pipeline', operator: 'EQ', value: pipelineId },
          { propertyName: 'dealstage', operator: 'EQ', value: stageId },
        ],
      },
    ],
    properties: [
      'dealname', 'dealstage', 'amount', 'camelot_estimated_units',
      'camelot_prospect_score', 'camelot_outreach_angle', 'createdate',
    ],
    sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }],
    limit: 100,
  };

  const deals = [];
  let after = null;

  do {
    if (after) payload.after = after;
    const { data } = await withRetry(() =>
      hubspotRequest('POST', '/crm/v3/objects/deals/search', payload)
    );
    deals.push(...(data.results || []));
    after = data.paging?.next?.after || null;
  } while (after);

  return deals;
}

/**
 * Search deals in the Roll-Up pipeline by company name.
 *
 * @param {string} query  Company name fragment to search
 * @returns {Promise<Array>} Matching deal objects
 */
async function searchDeals(query) {
  const { pipelineId } = await getPipelineInfo();

  const payload = {
    filterGroups: [
      {
        filters: [
          { propertyName: 'pipeline', operator: 'EQ', value: pipelineId },
          { propertyName: 'dealname', operator: 'CONTAINS_TOKEN', value: query },
        ],
      },
    ],
    properties: ['dealname', 'dealstage', 'amount', 'camelot_estimated_units'],
    limit: 20,
  };

  const { data } = await withRetry(() =>
    hubspotRequest('POST', '/crm/v3/objects/deals/search', payload)
  );

  return data.results || [];
}

/**
 * Get full pipeline summary: deal count and total value per stage.
 *
 * @returns {Promise<object>} { stageName: { count, totalValue } }
 */
async function getPipelineSummary() {
  const { stages } = await getPipelineInfo();
  const summary = {};

  for (const [label] of Object.entries(stages)) {
    try {
      const deals = await getDealsInStage(label);
      const totalValue = deals.reduce((sum, d) => {
        return sum + parseFloat(d.properties?.amount || 0);
      }, 0);
      summary[label] = { count: deals.length, totalValue };
    } catch {
      summary[label] = { count: 0, totalValue: 0 };
    }
  }

  return summary;
}

// ---------------------------------------------------------------------------
// Module exports
// ---------------------------------------------------------------------------

module.exports = {
  upsertProspect,
  logOutreach,
  updateDealStage,
  getDeal,
  getDealsInStage,
  searchDeals,
  getPipelineSummary,
  getPipelineInfo,
  // Lower-level helpers (useful for testing)
  upsertCompany,
  upsertContact,
  findExistingDeal,
};

// ---------------------------------------------------------------------------
// CLI entry point (for testing)
// ---------------------------------------------------------------------------

if (require.main === module) {
  const [, , command, ...rest] = process.argv;

  async function main() {
    switch (command) {
      case 'summary': {
        const summary = await getPipelineSummary();
        console.log('\n=== Camelot Roll-Up Pipeline Summary ===');
        for (const [stage, { count, totalValue }] of Object.entries(summary)) {
          console.log(
            `  ${stage.padEnd(20)} ${String(count).padStart(4)} deals   $${totalValue.toLocaleString()}`
          );
        }
        break;
      }
      case 'stage': {
        const stage = rest[0] || 'Identified';
        const deals = await getDealsInStage(stage);
        console.log(`\n=== Deals in "${stage}" ===`);
        for (const d of deals) {
          console.log(
            `  [${d.id}] ${d.properties.dealname} — ` +
            `${d.properties.camelot_estimated_units || '?'} units — ` +
            `score: ${d.properties.camelot_prospect_score || '?'}`
          );
        }
        break;
      }
      case 'get': {
        const dealId = rest[0];
        if (!dealId) { console.error('Usage: hubspot_pipeline.js get <dealId>'); process.exit(1); }
        const deal = await getDeal(dealId);
        console.log(JSON.stringify(deal, null, 2));
        break;
      }
      case 'search': {
        const query = rest.join(' ');
        if (!query) { console.error('Usage: hubspot_pipeline.js search <query>'); process.exit(1); }
        const deals = await searchDeals(query);
        console.log(`Found ${deals.length} deals:`);
        for (const d of deals) {
          console.log(`  [${d.id}] ${d.properties.dealname}`);
        }
        break;
      }
      default:
        console.log('Usage: node hubspot_pipeline.js [summary|stage <name>|get <id>|search <query>]');
    }
  }

  main().catch((err) => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}
