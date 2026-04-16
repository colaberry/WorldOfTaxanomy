import { revalidatePath } from 'next/cache'
import { NextRequest, NextResponse } from 'next/server'

/**
 * On-demand cache invalidation endpoint.
 *
 * Called by backend ingesters after data changes to bust the
 * Next.js server-side cache. Protected by a shared secret.
 *
 * POST /api/revalidate
 * Headers: x-revalidate-secret: <secret>
 * Body: { "systemId": "naics_2022" } or { "scope": "all" }
 */
export async function POST(request: NextRequest) {
  const secret = request.headers.get('x-revalidate-secret')
  const expected = process.env.REVALIDATE_SECRET

  if (!expected || secret !== expected) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const body = await request.json()
  const { systemId, scope } = body as { systemId?: string; scope?: string }

  if (scope === 'all') {
    // Revalidate all pages by busting the layout cache
    revalidatePath('/', 'layout')
    return NextResponse.json({ revalidated: true, scope: 'all' })
  }

  if (systemId) {
    // Revalidate specific system page and related pages
    revalidatePath(`/system/${systemId}`, 'page')
    revalidatePath('/dashboard', 'page')
    revalidatePath('/', 'page')
    return NextResponse.json({ revalidated: true, systemId })
  }

  return NextResponse.json({ error: 'Provide systemId or scope=all' }, { status: 400 })
}
