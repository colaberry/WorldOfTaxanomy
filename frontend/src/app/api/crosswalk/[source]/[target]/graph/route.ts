import { NextResponse } from 'next/server'
import { getStaticGraph } from '@/lib/crosswalk-data'

export async function GET(
  req: Request,
  { params }: { params: Promise<{ source: string; target: string }> },
) {
  const { source, target } = await params
  const url = new URL(req.url)
  const limit = Number(url.searchParams.get('limit') ?? '1000')
  const section = url.searchParams.get('section') ?? undefined

  const data = getStaticGraph(source, target, limit, section)
  if (!data) {
    return NextResponse.json({
      source_system: source, target_system: target,
      nodes: [], edges: [], total_edges: 0, truncated: false,
    })
  }
  return NextResponse.json(data)
}
